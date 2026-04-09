"""Pipeline principal de validação.

Fluxo:
  1. Autentica no Club
  2. Lista pedidos do D-1
  3. Para cada OC (em paralelo limitado):
     - get_concorrentes, get_produtos_cotacao, get_order_details
     - consulta Cilia
     - consulta card no Pipefy + baixa/parseia PDF
     - aplica R1..R6
     - decide fase destino
  4. Persiste resultados em SQLite
  5. Atualiza cards no Pipefy (se não dry_run)
  6. Gera relatório HTML + Excel
  7. Envia e-mails de divergência (se configurado)
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.clients.club_client import ClubClient
from app.clients.cilia_client import CiliaClient, build_cilia_client
from app.clients.pipefy_client import PipefyClient
from app.config import settings
from app.db import (
    atualizar_cache_cancelamentos,
    atualizar_cache_devolucoes,
    buscar_todas_duplicidades_placa,
    get_cancelamentos_por_placa,
    get_devolucoes_por_oc,
    get_devolucoes_por_placa,
    init_db,
    registrar_acao_planejada,
    registrar_historico_produtos,
    registrar_oc_resultado,
    registrar_validacao,
)
from app.logging_setup import get_logger
from app.models import (
    CardPipefy,
    Concorrente,
    ContextoValidacao,
    FasePipefy,
    Fornecedor,
    ItemOC,
    OcOrfa,
    OrdemCompra,
    ProdutoCotacao,
    ResultadoValidacao,
    Severidade,
    StatusValidacao,
)
from app.services import compradores as compradores_svc
from app.services.historico_produtos import (
    _chave_produto_dict,
    garantir_historico,
)
from app.validators import REGRAS_PADRAO, aplicar_regras
from app.validators.r2_duplicidade import detectar_reincidencias

logger = get_logger(__name__)


# ======================================================================
# Helpers de parsing
# ======================================================================

def _to_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v)).quantize(Decimal("0.01"))
    except Exception:
        return None


def _parse_data(v: Any) -> date | None:
    if not v:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    # ISO completo (com timezone) — cobre "2026-04-06T13:00:00Z" e variantes
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    # Formatos explícitos
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_oc(raw: dict[str, Any]) -> OrdemCompra:
    forn_raw = raw.get("fornecedor") or {}
    fornecedor = Fornecedor(
        for_id=str(raw.get("for_id") or forn_raw.get("for_id") or "") or None,
        for_nome=forn_raw.get("for_nome") or raw.get("fornecedor_nome"),
        for_status=str(forn_raw.get("for_status") or "") or None,
        for_excluido=str(forn_raw.get("for_excluido") or "0"),
        for_cnpj=forn_raw.get("for_cnpj"),
    )
    items_raw = raw.get("items") or raw.get("itens") or []
    items = [
        ItemOC(
            product_id=str((it.get("product") or {}).get("id") or it.get("produto_id") or "") or None,
            descricao=(it.get("product") or {}).get("name") or it.get("descricao"),
            quantity=it.get("quantity") or it.get("quantidade") or 0,
            unit_price=_to_decimal(it.get("unit_price") or it.get("valor_unitario")),
            total_price=_to_decimal(it.get("total_price") or it.get("valor_total")),
        )
        for it in items_raw
    ]

    data_pedido = _parse_data(
        raw.get("data_pedido")
        or raw.get("generation_date")
        or raw.get("data")
    )

    created_by_raw = raw.get("created_by")
    try:
        created_by = int(created_by_raw) if created_by_raw is not None else None
    except (ValueError, TypeError):
        created_by = None

    return OrdemCompra(
        id_pedido=str(raw.get("id_pedido") or raw.get("id") or ""),
        id_cotacao=str(raw.get("id_cotacao") or raw.get("number_quote") or "") or None,
        identificador=raw.get("identificador") or raw.get("identifier"),
        valor_pedido=_to_decimal(raw.get("valor_pedido") or raw.get("value")),
        forma=raw.get("forma") or ((raw.get("payment_term") or {}).get("ttp_nome")),
        created_by=created_by,
        usu_nome_club=raw.get("usu_nome") or raw.get("requester"),
        status=raw.get("status"),
        fornecedor=fornecedor,
        divergencia_flag=bool(raw.get("divergencia")),
        data_pedido=data_pedido,
        items=items,
    )


def _resumir_cancelamento(placa_normalizada: str) -> tuple[str, str | None]:
    """Retorna (label, card_id) do estado de cancelamento da placa.
    label ∈ {"—", "info_incorretas", "cancelado", "ambos"}.
    Quando há "ambos", prioriza o card_id de "informacoes_incorretas"
    porque é o estado ATIVO (em revisão), mais útil para o analista."""
    cancs = get_cancelamentos_por_placa(placa_normalizada)
    if not cancs:
        return "—", None
    tipos = {c["tipo"] for c in cancs}
    if "informacoes_incorretas" in tipos and "cancelado" in tipos:
        # Pega o card_id do tipo ativo
        ativo = next(c for c in cancs if c["tipo"] == "informacoes_incorretas")
        return "ambos", ativo["card_id"]
    if "informacoes_incorretas" in tipos:
        return "info_incorretas", cancs[0]["card_id"]
    return "cancelado", cancs[0]["card_id"]


def _resumir_reincidencia_de_divs(divergencias_cross: list) -> str:
    """A partir das divergências cross-time já geradas, retorna o label
    do mais grave:
      sim_sem_devolucao > sim_devolucao_outra_peca > sim_mesmo_forn
      > sim_outro_forn > sim_com_devolucao_peca > —

    Prioridade: peça duplicada SEM devolução é o alerta mais grave.
    """
    if not divergencias_cross:
        return "—"

    # Peça duplicada sem NENHUMA devolução (nem da placa)
    tem_sem_dev = any(
        (d.dados or {}).get("sem_devolucao")
        for d in divergencias_cross
    )
    if tem_sem_dev:
        # Verifica se alguma é mesmo fornecedor (mais grave)
        tem_mesmo_sem = any(
            (d.dados or {}).get("sem_devolucao")
            and (d.dados or {}).get("mesmo_fornecedor")
            for d in divergencias_cross
        )
        return "sim_sem_devolucao_mesmo_forn" if tem_mesmo_sem else "sim_sem_devolucao"

    # Há devolução da OC anterior, mas de OUTRA peça
    tem_dev_outra = any(
        (d.dados or {}).get("tem_devolucao_outra_peca")
        for d in divergencias_cross
    )
    if tem_dev_outra:
        return "sim_devolucao_outra_peca"

    # Mesmo fornecedor com devolução da peça
    tem_mesmo = any(
        (d.dados or {}).get("mesmo_fornecedor")
        and (d.dados or {}).get("tem_devolucao_peca")
        for d in divergencias_cross
    )
    if tem_mesmo:
        return "sim_mesmo_forn"

    # Outro fornecedor com devolução da peça
    tem_outro = any(
        not (d.dados or {}).get("mesmo_fornecedor")
        and (d.dados or {}).get("tem_devolucao_peca")
        for d in divergencias_cross
    )
    if tem_outro:
        return "sim_outro_forn"

    return "sim_com_devolucao_peca"


def _computar_duplicidades_placa(
    placa_normalizada: str,
    data_d1_str: str,
    chaves_oc_atual: set[str],
) -> list[dict]:
    """Busca TODAS as peças duplicadas (2+ ocorrências em 90d) para a placa,
    EXCLUINDO as que já estão na OC atual (essas aparecem na R2 cross-time).
    Para cada duplicidade, verifica se há devolução no cache.

    Retorna lista de dicts prontos para o template."""
    from app.config import settings
    todas = buscar_todas_duplicidades_placa(
        placa_normalizada,
        data_max=data_d1_str,
        dias=settings.r2_janela_dias,
    )
    resultado = []
    for dup in todas:
        chave = dup["chave_produto"]
        # Pular peças que JÁ estão na OC atual (já aparecem na R2 cross-time)
        if chave in chaves_oc_atual:
            continue

        # Enriquecer com status de devolução
        ids_pedido = (dup.get("ids_pedido") or "").split("|")
        datas_oc = (dup.get("datas_oc") or "").split("|")
        fornecedores = (dup.get("fornecedores") or "").split("|")
        cards_pipefy = (dup.get("cards_pipefy") or "").split("|")

        # Verificar devolução para cada OC anterior
        tem_devolucao = False
        card_devolucao_id = None
        link_devolucao = None
        for id_pedido in ids_pedido:
            devs = get_devolucoes_por_oc(id_pedido.strip())
            if devs:
                tem_devolucao = True
                card_devolucao_id = devs[0]["card_id"]
                link_devolucao = (
                    f"https://app.pipefy.com/pipes/{settings.pipefy_pipe_devolucao_id}"
                    f"#cards/{card_devolucao_id}"
                )
                break

        # Montar links para OCs no Pipefy
        ocs_info = []
        for i, id_p in enumerate(ids_pedido):
            card_id = cards_pipefy[i].strip() if i < len(cards_pipefy) else ""
            link_card = (
                f"https://app.pipefy.com/pipes/{settings.pipe_id}#cards/{card_id}"
                if card_id and card_id != "None"
                else None
            )
            ocs_info.append({
                "id_pedido": id_p.strip(),
                "data_oc": datas_oc[i].strip() if i < len(datas_oc) else "?",
                "fornecedor": fornecedores[i].strip() if i < len(fornecedores) else "?",
                "link_card": link_card,
            })

        resultado.append({
            "chave_produto": chave,
            "descricao": dup.get("descricao") or chave,
            "total_ocorrencias": dup["total_ocorrencias"],
            "ocs": ocs_info,
            "tem_devolucao": tem_devolucao,
            "card_devolucao_id": card_devolucao_id,
            "link_devolucao": link_devolucao,
        })
    return resultado


def _verificar_duplicidade_interna(produtos: list[ProdutoCotacao]) -> str:
    """Aplica a parte 1 da R2 (peças repetidas pela mesma chave) numa
    lista de produtos. Retorna 'Sim' / 'Não' / '—' (sem produtos).

    Reusa a mesma lógica de chave usada em `r2_duplicidade._chave_produto`
    (EAN > código interno > descrição normalizada). Não importamos a
    função para evitar dependência circular do validators no orchestrator.
    """
    if not produtos:
        return "—"
    from collections import defaultdict
    grupos: dict[str, int] = defaultdict(int)
    for p in produtos:
        ean = (p.ean or "").strip()
        cod = (p.cod_interno or "").strip()
        desc = (p.descricao or "").strip().lower()
        if ean:
            chave = f"ean:{ean}"
        elif cod:
            chave = f"cod:{cod}"
        else:
            chave = f"desc:{desc}"
        grupos[chave] += 1
    return "Sim" if any(c > 1 for c in grupos.values()) else "Não"


def _eh_mercado_livre(oc: OrdemCompra, card: CardPipefy | None) -> bool:
    """ML é detectado pelo campo 'Origem da peça' do card no Pipefy
    (fonte canônica). Como fallback, usa o nome do fornecedor do Club —
    útil para OCs órfãs (sem card)."""
    if card and card.eh_mercado_livre:
        return True
    return oc.eh_mercado_livre


def _decidir_fase(
    oc: OrdemCompra,
    card: CardPipefy | None,
    divergencias: list,
) -> FasePipefy | None:
    """Decide a fase destino do card no Pipefy.

    Regras (em ordem):
      1. Mercado Livre → Compras Mercado Livre (movemos o card; o status
         AGUARDANDO_ML continua sinalizando "validação manual" no relatório).
      2. Divergência bloqueante (ERRO/ALERTA) → Informações Incorretas.
      3. PIX → Programar Pagamento.
      4. Cartão de Crédito / Faturado / Boleto → Aguardar Peças.
      5. Forma de pagamento ausente ou desconhecida → None (não move; o
         card fica em Validação para o analista revisar manualmente).

    A "Forma de pagamento" é lida do CARD do Pipefy (`card.forma_pagamento`),
    não do Club. Os valores possíveis (segundo o pipefy_ids/start form) são
    exatamente: PIX, Cartão de Crédito, Faturado, Boleto.
    """
    if _eh_mercado_livre(oc, card):
        return FasePipefy.COMPRAS_ML

    if any(d.severidade in (Severidade.ERRO, Severidade.ALERTA) for d in divergencias):
        return FasePipefy.INFORMACOES_INCORRETAS

    forma = (card.forma_pagamento if card else None) or ""
    forma_norm = forma.strip().lower()
    if not forma_norm:
        # Sem forma de pagamento preenchida no card — não move automaticamente.
        # O analista precisa preencher antes ou intervir manualmente.
        logger.warning(
            "OC %s: card sem 'Forma de pagamento' preenchida — não será movido",
            oc.id_pedido,
        )
        return None
    if "pix" in forma_norm:
        return FasePipefy.PROGRAMAR_PAGAMENTO
    if (
        "cart" in forma_norm           # "Cartão de Crédito"
        or "faturado" in forma_norm
        or "boleto" in forma_norm
    ):
        return FasePipefy.AGUARDAR_PECAS

    logger.warning(
        "OC %s: forma de pagamento desconhecida no card: %r — não será movido",
        oc.id_pedido, forma,
    )
    return None


FASE_ENUM_PARA_CHAVE = {
    FasePipefy.AGUARDAR_PECAS: "aguardar_pecas",
    FasePipefy.PROGRAMAR_PAGAMENTO: "programar_pagamento",
    FasePipefy.COMPRAS_ML: "compras_ml",
    FasePipefy.INFORMACOES_INCORRETAS: "informacoes_incorretas",
}


# ======================================================================
# Coleta paralela por OC
# ======================================================================

@dataclass
class ColetaOC:
    """Dados coletados de uma OC de todas as fontes, antes das regras."""
    oc: OrdemCompra
    concorrentes: list[Concorrente]
    produtos_cotacao: list[ProdutoCotacao]
    orcamento_cilia: Any
    card_pipefy: CardPipefy | None


def _oc_minima_para_card_orfao(card: CardPipefy) -> OrdemCompra:
    """Sintetiza uma OrdemCompra mínima para um card que NÃO foi
    encontrado no Club (codigo_oc inválido ou OC não existe). Permite
    que o card ainda apareça no relatório com divergência ERRO.
    """
    return OrdemCompra(
        id_pedido=card.codigo_oc or f"card:{card.id}",
        id_cotacao=None,
        identificador=card.title or None,
        valor_pedido=None,
        forma=None,
        created_by=None,
        fornecedor=None,
        data_pedido=None,
        items=[],
    )


async def _coletar_para_card(
    card: CardPipefy,
    raw_oc: dict[str, Any] | None,
    club: ClubClient,
    cilia: CiliaClient,
    pipefy: PipefyClient,
) -> ColetaOC:
    """Coleta dados de validação para um CARD do Pipefy.

    Se `raw_oc` foi encontrada no índice do Club, enriquece com
    concorrentes/produtos/detalhes. Caso contrário, retorna uma OC
    mínima — a regra R3 vai gerar diverência "valor do Club ausente".
    """
    if raw_oc is None:
        oc_basico = _oc_minima_para_card_orfao(card)
        # Cilia ainda pode ser consultado por placa (extraída do card)
        orcamento = None
        if oc_basico.placa_normalizada:
            try:
                orcamento = await cilia.consultar_por_placa(oc_basico.placa_normalizada)
            except Exception as e:
                logger.warning("Falha Cilia para card órfão %s: %s", card.id, e)

        if card.anexo_oc_url and card.valor_extraido_pdf is None:
            try:
                await pipefy.extrair_valor_pdf(card)
            except Exception as e:
                logger.warning("Falha PDF card %s: %s", card.id, e)

        return ColetaOC(
            oc=oc_basico,
            concorrentes=[],
            produtos_cotacao=[],
            orcamento_cilia=orcamento,
            card_pipefy=card,
        )

    oc_basico = _parse_oc(raw_oc)

    tarefas = {}
    if oc_basico.id_cotacao:
        tarefas["concorrentes"] = club.get_concorrentes(oc_basico.id_cotacao)
    tarefas["detalhes"] = club.get_order_details(oc_basico.id_pedido)
    if oc_basico.placa_normalizada:
        tarefas["cilia"] = cilia.consultar_por_placa(oc_basico.placa_normalizada)

    resultados = await asyncio.gather(*tarefas.values(), return_exceptions=True)
    res_map = dict(zip(tarefas.keys(), resultados))

    def _ok(key: str):
        v = res_map.get(key)
        if isinstance(v, Exception):
            logger.warning("Falha em %s para OC %s: %s", key, oc_basico.id_pedido, v)
            return None
        return v

    concorrentes_raw = _ok("concorrentes") or []
    detalhes = _ok("detalhes") or {}
    orcamento = _ok("cilia")

    if detalhes:
        if not oc_basico.valor_pedido:
            oc_basico.valor_pedido = _to_decimal(detalhes.get("value"))
        if not oc_basico.items and detalhes.get("items"):
            oc_basico = _parse_oc({**raw_oc, **detalhes})

    concorrentes = [
        Concorrente(
            id_fornecedor=str(c.get("id_fornecedor") or ""),
            fornecedor_nome=c.get("fornecedor_nome"),
        )
        for c in concorrentes_raw
    ]
    # Os produtos do contexto agora vêm dos ITENS efetivamente comprados
    # nesta OC específica (`detalhes.items`), não da cotação inteira
    # (que era o que `get_produtos_cotacao` retornava). Quando várias
    # OCs compartilham a mesma cotação, o método antigo populava cada
    # OC com TODOS os produtos cotados — gerando falsos positivos
    # enormes na R2 cross-time. O novo método respeita o que foi de
    # fato pedido por OC, alinhado com o relatório `Club > Produtos`.
    produtos = [
        ProdutoCotacao(
            produto_id=str((it.get("product") or {}).get("id") or ""),
            descricao=(it.get("product") or {}).get("name"),
            quantidade=float(it.get("quantity") or 0),
            ean=(it.get("product") or {}).get("ean"),
            cod_interno=(it.get("product") or {}).get("internal_code"),
        )
        for it in (detalhes.get("items") or [])
    ]

    if card.anexo_oc_url and card.valor_extraido_pdf is None:
        try:
            await pipefy.extrair_valor_pdf(card)
        except Exception as e:
            logger.warning("Falha PDF card %s: %s", card.id, e)

    return ColetaOC(
        oc=oc_basico,
        concorrentes=concorrentes,
        produtos_cotacao=produtos,
        orcamento_cilia=orcamento,
        card_pipefy=card,
    )


# ======================================================================
# Aplicação das ações no Pipefy
# ======================================================================

async def _atuar_no_pipefy(
    pipefy: PipefyClient,
    resultado: ResultadoValidacao,
    validacao_id: int,
) -> None:
    """Aplica (ou apenas REGISTRA, em modo consulta) as ações planejadas
    no Pipefy para uma OC validada.

    Em `MODO_OPERACAO=consulta`: nenhuma chamada externa é feita; cada
    ação é gravada em `acoes_pipefy_planejadas` com `executada=0` para
    auditoria preventiva.

    Em `MODO_OPERACAO=automatico`: executa as mutations e grava com
    `executada=1` (ou `executada=0` + `erro=...` se a chamada falhar).
    """
    if not resultado.card_pipefy_id:
        logger.debug("OC %s sem card no Pipefy — pula atuação", resultado.oc.id_pedido)
        return

    # ML aguarda validação manual; JA_PROCESSADA significa que o card já
    # está em outra fase (muitos campos de validação não existem lá).
    if not resultado.requer_acao_pipefy:
        logger.info(
            "Skip Pipefy: OC %s status=%s fase_atual=%s",
            resultado.oc.id_pedido,
            resultado.status.value,
            resultado.fase_pipefy_atual,
        )
        return

    is_auto = settings.modo_operacao == "automatico"

    # Lista de ações planejadas: (acao, payload_dict, motivo)
    acoes: list[tuple[str, dict[str, Any], str | None]] = [
        ("update_field", {"campo": "peca_duplicada", "valor": resultado.peca_duplicada}, None),
        ("update_field", {"campo": "abatimento_fornecedor", "valor": resultado.abatimento_fornecedor}, None),
        ("update_field", {"campo": "validacao_concluida_por", "valor": settings.validador_identificador}, None),
        ("update_field", {"campo": "validacao_concluida", "valor": "Sim" if resultado.aprovada else "Não"}, None),
    ]

    if resultado.divergencias:
        texto = "\n".join(
            f"[{d.regra}] {d.titulo}: {d.descricao}"
            for d in resultado.divergencias
        )
        acoes.append((
            "update_field",
            {"campo": "justificativa_divergencia", "valor": texto},
            "registrar divergências no card",
        ))

    if resultado.fase_destino:
        chave = FASE_ENUM_PARA_CHAVE.get(resultado.fase_destino)
        # Não-op se o card já está na fase destino (caso típico: card
        # reaberto manualmente em "Informações Incorretas" que volta a
        # ter divergência → fase_destino = INFORMACOES_INCORRETAS).
        ja_na_fase_destino = (
            resultado.fase_pipefy_atual
            and resultado.fase_pipefy_atual.strip().lower()
            == resultado.fase_destino.value.strip().lower()
        )
        if chave and not ja_na_fase_destino:
            acoes.append((
                "move_card",
                {"fase_destino": resultado.fase_destino.value, "chave": chave},
                f"status={resultado.status.value}",
            ))
        elif ja_na_fase_destino:
            logger.info(
                "Card %s já está em '%s' — pula move_card",
                resultado.card_pipefy_id, resultado.fase_pipefy_atual,
            )

    for acao, payload, motivo in acoes:
        if not is_auto:
            registrar_acao_planejada(
                validacao_id=validacao_id,
                oc_numero=resultado.oc.id_pedido,
                card_id=resultado.card_pipefy_id,
                acao=acao,
                payload=payload,
                motivo=motivo,
                executada=False,
            )
            logger.info(
                "MODO CONSULTA: ação %s registrada (NÃO executada) — OC=%s card=%s",
                acao, resultado.oc.id_pedido, resultado.card_pipefy_id,
            )
            continue

        # Modo automatico — executar de fato
        try:
            if acao == "update_field":
                await pipefy.update_card_field(
                    resultado.card_pipefy_id, payload["campo"], payload["valor"]
                )
            elif acao == "move_card":
                await pipefy.mover_card(resultado.card_pipefy_id, payload["chave"])
            registrar_acao_planejada(
                validacao_id=validacao_id,
                oc_numero=resultado.oc.id_pedido,
                card_id=resultado.card_pipefy_id,
                acao=acao,
                payload=payload,
                motivo=motivo,
                executada=True,
            )
        except Exception as e:
            logger.error(
                "Falha em ação %s para OC %s: %s",
                acao, resultado.oc.id_pedido, e,
            )
            registrar_acao_planejada(
                validacao_id=validacao_id,
                oc_numero=resultado.oc.id_pedido,
                card_id=resultado.card_pipefy_id,
                acao=acao,
                payload=payload,
                motivo=motivo,
                executada=False,
                erro=str(e),
            )


# ======================================================================
# Orquestrador principal
# ======================================================================

async def executar_validacao(
    data_d1: date,
    *,
    dry_run: bool = True,
    concorrencia: int = 5,
) -> tuple[int, list[ResultadoValidacao], list[OcOrfa]]:
    """Executa o pipeline completo no novo fluxo INVERTIDO:

      1. Lista OCs do Club do D-1 → indexa por id_pedido
      2. Lista cards do Pipefy nas fases relevantes (validação + destinos)
      3. Filtra cards na fase "validacao" cuja `created_at` seja em D-1
      4. Para cada card filtrado, busca a OC no índice por
         `card.codigo_oc == oc.id_pedido` (1:1)
      5. Cards sem OC → ResultadoValidacao com OC mínima + divergência
         ERRO 'OC não encontrada no Club' (gerada pelo R3)
      6. OCs do Club que NÃO foram consumidas por nenhum card → lista
         paralela de `OcOrfa` no relatório

    Retorna (validacao_id, resultados, ocs_orfas).
    """
    init_db()
    logger.info(
        "=== Início validação D-1=%s dry_run=%s ===", data_d1, dry_run
    )

    cilia = build_cilia_client()

    async with ClubClient() as club, PipefyClient(dry_run=dry_run) as pipefy:
        # 1. Indexar OCs do Club do D-1 por id_pedido
        pedidos_raw = await club.listar_pedidos(data_d1)
        logger.info("Club: %d OCs encontradas em %s", len(pedidos_raw), data_d1)
        ocs_index: dict[str, dict[str, Any]] = {}
        for raw in pedidos_raw:
            id_pedido = str(raw.get("id_pedido") or raw.get("id") or "").strip()
            if id_pedido:
                ocs_index[id_pedido] = raw

        # 2. Listar cards de TODAS as fases relevantes (precisamos das
        # fases destino para detectar cards já processados em ciclos
        # anteriores e marcar como JA_PROCESSADA).
        fases_para_scan = [
            "validacao",
            "aguardar_pecas",
            "programar_pagamento",
            "compras_ml",
            "informacoes_incorretas",
        ]
        todos_cards: list[CardPipefy] = []
        for fase_chave in fases_para_scan:
            try:
                cards_fase = await pipefy.listar_cards_fase(fase_chave)
            except Exception as e:
                logger.warning("Falha ao listar fase %s: %s", fase_chave, e)
                continue
            todos_cards.extend(cards_fase)
        logger.info("Pipefy: %d cards lidos em %d fases", len(todos_cards), len(fases_para_scan))

        # 3. Filtrar cards: criados em D-1 (independente da fase atual).
        # O filtro de data se aplica ao `created_at` do card no Pipefy,
        # não à fase — o objetivo é validar o que foi gerado naquele dia,
        # mesmo que o analista já tenha movido o card.
        cards_do_dia: list[CardPipefy] = []
        for c in todos_cards:
            if c.created_at and c.created_at.date() == data_d1:
                cards_do_dia.append(c)
        logger.info(
            "Pipefy: %d cards filtrados por created_at == %s",
            len(cards_do_dia), data_d1,
        )

        # 4. Coleta paralela por CARD (com semáforo).
        # Para cada card, busca a OC no índice por codigo_oc == id_pedido.
        semaforo = asyncio.Semaphore(concorrencia)
        ids_pedido_consumidos: set[str] = set()

        async def _com_sem(card: CardPipefy):
            async with semaforo:
                raw = None
                if card.codigo_oc:
                    raw = ocs_index.get(card.codigo_oc.strip())
                    if raw is not None:
                        ids_pedido_consumidos.add(card.codigo_oc.strip())
                return await _coletar_para_card(card, raw, club, cilia, pipefy)

        coletas = await asyncio.gather(*[_com_sem(c) for c in cards_do_dia])

        # 4b. Resolver nome/email dos compradores
        compradores_svc.init_table()
        for coleta in coletas:
            cb = coleta.oc.created_by
            if cb:
                nome, email = compradores_svc.resolve(cb)
                coleta.oc.comprador_nome = nome
                coleta.oc.comprador_email = email

        # 4c. Calcular OCs órfãs (no Club mas sem card no Pipefy) e
        # rodar a verificação de peça duplicada (R2 parte 1) também nelas.
        # Buscamos `produtos_cotacao` por id_cotacao em paralelo para cada
        # órfã com semáforo, para não saturar o Club.
        orfas_raw: list[tuple[OrdemCompra, str | None]] = []
        for id_pedido, raw in ocs_index.items():
            if id_pedido in ids_pedido_consumidos:
                continue
            oc = _parse_oc(raw)
            comprador = None
            if oc.created_by:
                nome, _ = compradores_svc.resolve(oc.created_by)
                comprador = nome
            orfas_raw.append((oc, comprador))

        async def _produtos_orfa(oc: OrdemCompra) -> list[ProdutoCotacao]:
            """Busca os ITENS efetivamente comprados na OC órfã via
            `get_order_details(id_pedido).items` (NÃO `get_produtos_cotacao`,
            que retorna a cotação inteira e gera falsos positivos quando
            várias OCs compartilham a mesma cotação)."""
            async with semaforo:
                try:
                    det = await club.get_order_details(oc.id_pedido)
                except Exception as e:
                    logger.warning(
                        "Falha get_order_details para OC órfã %s: %s",
                        oc.id_pedido, e,
                    )
                    return []
            items = det.get("items") or []
            return [
                ProdutoCotacao(
                    produto_id=str(
                        (it.get("product") or {}).get("id") or ""
                    ),
                    descricao=(it.get("product") or {}).get("name"),
                    quantidade=float(it.get("quantity") or 0),
                    ean=(it.get("product") or {}).get("ean"),
                    cod_interno=(it.get("product") or {}).get("internal_code"),
                )
                for it in items
            ]

        produtos_por_orfa = await asyncio.gather(
            *[_produtos_orfa(oc) for oc, _ in orfas_raw]
        )
        logger.info(
            "Coletados produtos para %d OCs órfãs", len(orfas_raw)
        )

        # 4d. R2 cross-time — preparar histórico e cache de devoluções
        # antes de aplicar as regras. Backfill incremental: na 1ª execução
        # leva alguns minutos; nas subsequentes só baixa o D-1 atual.
        if settings.r2_modo != "off":
            try:
                await garantir_historico(
                    club,
                    ate_dia=data_d1,
                    dias_janela=settings.r2_janela_dias,
                )
            except Exception as e:
                logger.error("Falha no backfill de histórico: %s", e)

            # Persistir os produtos do D-1 atual no histórico (para que
            # execuções futuras já encontrem o D-1 de hoje como histórico)
            linhas_d1: list[dict[str, Any]] = []
            data_iso = data_d1.isoformat()
            # OCs com card (estão em `coletas`)
            for coleta in coletas:
                oc = coleta.oc
                if not coleta.produtos_cotacao:
                    continue
                forn_id = (
                    oc.fornecedor.for_id if oc.fornecedor else None
                )
                forn_nome = (
                    oc.fornecedor.for_nome if oc.fornecedor else None
                )
                placa_norm = oc.placa_normalizada
                for p in coleta.produtos_cotacao:
                    linhas_d1.append({
                        "data_oc": data_iso,
                        "id_pedido": oc.id_pedido,
                        "id_cotacao": oc.id_cotacao,
                        "placa_normalizada": placa_norm,
                        "identificador": oc.identificador,
                        "chave_produto": _chave_produto_dict({
                            "ean": p.ean,
                            "cod_interno": p.cod_interno,
                            "descricao": p.descricao,
                        }),
                        "descricao": p.descricao,
                        "fornecedor_id": str(forn_id) if forn_id else None,
                        "fornecedor_nome": forn_nome,
                        "quantidade": float(p.quantidade or 0),
                        "card_pipefy_id": (
                            coleta.card_pipefy.id if coleta.card_pipefy else None
                        ),
                    })
            # OCs órfãs (já temos os produtos coletados em `produtos_por_orfa`)
            for (oc, _comprador), produtos in zip(orfas_raw, produtos_por_orfa):
                forn_id = oc.fornecedor.for_id if oc.fornecedor else None
                forn_nome = oc.fornecedor.for_nome if oc.fornecedor else None
                for p in produtos:
                    linhas_d1.append({
                        "data_oc": data_iso,
                        "id_pedido": oc.id_pedido,
                        "id_cotacao": oc.id_cotacao,
                        "placa_normalizada": oc.placa_normalizada,
                        "identificador": oc.identificador,
                        "chave_produto": _chave_produto_dict({
                            "ean": p.ean,
                            "cod_interno": p.cod_interno,
                            "descricao": p.descricao,
                        }),
                        "descricao": p.descricao,
                        "fornecedor_id": str(forn_id) if forn_id else None,
                        "fornecedor_nome": forn_nome,
                        "quantidade": float(p.quantidade or 0),
                        "card_pipefy_id": None,
                    })
            if linhas_d1:
                inseridas = registrar_historico_produtos(linhas_d1)
                logger.info(
                    "Histórico D-1: %d linhas registradas (de %d)",
                    inseridas, len(linhas_d1),
                )

            # Atualizar cache de devoluções a partir do pipe Devolução
            try:
                devs = await pipefy.listar_devolucoes_abertas()
                atualizar_cache_devolucoes(devs)
            except Exception as e:
                logger.error("Falha ao atualizar cache de devoluções: %s", e)

            # Atualizar cache de cancelamentos do PIPE PRINCIPAL
            # (fases Informações Incorretas + Cancelados)
            try:
                cancs = await pipefy.listar_cards_cancelamento_pipe_principal()
                atualizar_cache_cancelamentos(cancs)
            except Exception as e:
                logger.error("Falha ao atualizar cache de cancelamentos: %s", e)

        # 4e. Construir OcOrfas APLICANDO a R2 cross-time (agora que o
        # cache de devoluções e o histórico estão prontos). Para cards
        # com OC no Club, a R2 cross-time roda automaticamente dentro do
        # `aplicar_regras` mais abaixo. Para órfãs, precisamos rodar
        # manualmente porque elas não passam por `aplicar_regras`.
        ocs_orfas: list[OcOrfa] = []
        for (oc, comprador), produtos in zip(orfas_raw, produtos_por_orfa):
            peca_dup_interna = _verificar_duplicidade_interna(produtos)
            forn_id = oc.fornecedor.for_id if oc.fornecedor else None
            divs_cross = detectar_reincidencias(
                placa_normalizada=oc.placa_normalizada,
                identificador=oc.identificador,
                id_pedido_atual=oc.id_pedido,
                fornecedor_id=str(forn_id) if forn_id else None,
                produtos=produtos,
                data_d1=data_d1,
            )
            # Resumo de reincidência para a coluna do relatório
            reinc = _resumir_reincidencia_de_divs(divs_cross)
            cancel_label, cancel_card_id = _resumir_cancelamento(
                oc.placa_normalizada
            )
            chaves_reinc_orfa = sorted({
                str((d.dados or {}).get("chave_produto") or "")
                for d in divs_cross
                if (d.dados or {}).get("chave_produto")
            })
            # Chaves da OC atual para excluir das duplicidades históricas
            chaves_oc = set()
            for p in produtos:
                ean = (getattr(p, "ean", None) or "").strip()
                cod = (getattr(p, "cod_interno", None) or "").strip()
                desc = (getattr(p, "descricao", None) or "").strip().lower()
                if ean:
                    chaves_oc.add(f"ean:{ean}")
                elif cod:
                    chaves_oc.add(f"cod:{cod}")
                else:
                    chaves_oc.add(f"desc:{desc}")
            dups_placa = _computar_duplicidades_placa(
                oc.placa_normalizada, data_d1.isoformat(), chaves_oc,
            )
            ocs_orfas.append(
                OcOrfa(
                    id_pedido=oc.id_pedido,
                    id_cotacao=oc.id_cotacao,
                    identificador=oc.identificador,
                    valor=oc.valor_pedido,
                    fornecedor=oc.fornecedor.for_nome if oc.fornecedor else None,
                    comprador=comprador,
                    forma_pagamento=oc.forma,
                    data_pedido=oc.data_pedido,
                    peca_duplicada=peca_dup_interna,
                    qtd_produtos=len(produtos) if produtos else None,
                    divergencias=divs_cross,
                    reincidencia=reinc,
                    cancelamento=cancel_label,
                    cancelamento_card_id=cancel_card_id,
                    produtos=produtos,
                    chaves_reincidentes=chaves_reinc_orfa,
                    duplicidades_placa=dups_placa,
                )
            )
        logger.info(
            "OCs órfãs (Club sem card Pipefy): %d (com reincidência: %d)",
            len(ocs_orfas),
            sum(1 for o in ocs_orfas if o.reincidencia != "—"),
        )

        # ID da fase "Validação Ordem de Compra" — usado para detectar
        # cards que já saíram dela (status JA_PROCESSADA).
        fase_validacao_id = pipefy.ids.fase_id("validacao") if pipefy.ids else None

        # 4. Aplicar regras e construir resultados
        resultados: list[ResultadoValidacao] = []
        for coleta in coletas:
            contexto = ContextoValidacao(
                oc=coleta.oc,
                concorrentes=coleta.concorrentes,
                produtos_cotacao=coleta.produtos_cotacao,
                orcamento_cilia=coleta.orcamento_cilia,
                card_pipefy=coleta.card_pipefy,
                data_d1=data_d1,
            )
            divergencias = aplicar_regras(REGRAS_PADRAO, contexto)

            # Só divergências com severidade ERRO (ou ALERTA) reprovam.
            # INFO é só registro (ex: Cilia stub).
            bloqueantes = [
                d for d in divergencias
                if d.severidade in (Severidade.ERRO, Severidade.ALERTA)
            ]

            card = coleta.card_pipefy
            fase_atual_nome = card.phase_name if card else None

            # Card já está fora da fase "Validação"? (movido manualmente
            # ou em ciclo anterior) — usado como flag, NÃO mais como
            # short-circuit, para não engolir divergências de cards reabertos.
            card_movido = bool(
                card
                and card.phase_id
                and fase_validacao_id
                and card.phase_id != fase_validacao_id
            )

            # Hierarquia de status (ordem importa):
            #  1. ML — sempre, mesmo sem divergência
            #  2. Divergência bloqueante — SEMPRE prevalece, mesmo se card_movido
            #     (caso de card reaberto manualmente que voltou a ter problema)
            #  3. Card já fora da fase "Validação" + sem divergência — JA_PROCESSADA
            #  4. Aprovada
            if _eh_mercado_livre(coleta.oc, card):
                status = StatusValidacao.AGUARDANDO_ML
                # ML: AGORA movemos para "Compra Mercado Livre" (antes ficava
                # parado em Validação aguardando analista). O status
                # AGUARDANDO_ML continua sinalizando o caso no relatório.
                fase = FasePipefy.COMPRAS_ML
            elif bloqueantes:
                status = StatusValidacao.DIVERGENCIA
                fase = _decidir_fase(coleta.oc, card, divergencias)
            elif card_movido:
                status = StatusValidacao.JA_PROCESSADA
                fase = None
            else:
                status = StatusValidacao.APROVADA
                fase = _decidir_fase(coleta.oc, card, divergencias)

            # Peça duplicada: checa se R2 acusou duplicidade
            peca_dup = "Não"
            for d in divergencias:
                if d.regra == "R2" and "duplicada" in d.titulo.lower():
                    peca_dup = "Sim"
                    break

            # Subset das divergências cross-time (R2 parte 2)
            divs_cross_card = [
                d for d in divergencias
                if d.regra == "R2" and "cross-time" in d.titulo.lower()
            ]
            reinc_card = _resumir_reincidencia_de_divs(divs_cross_card)
            cancel_label, cancel_card_id = _resumir_cancelamento(
                coleta.oc.placa_normalizada
            )

            # Pré-computa o set de chaves de produtos reincidentes para
            # o template marcar visualmente cada peça (sem loop O(n*m)
            # no Jinja2). A chave é a mesma usada pela R2.
            chaves_reinc = sorted({
                str((d.dados or {}).get("chave_produto") or "")
                for d in divs_cross_card
                if (d.dados or {}).get("chave_produto")
            })

            # Duplicidades históricas da placa (peças NÃO na OC atual)
            chaves_oc_card = set()
            for p in coleta.produtos_cotacao:
                ean = (getattr(p, "ean", None) or "").strip()
                cod = (getattr(p, "cod_interno", None) or "").strip()
                desc = (getattr(p, "descricao", None) or "").strip().lower()
                if ean:
                    chaves_oc_card.add(f"ean:{ean}")
                elif cod:
                    chaves_oc_card.add(f"cod:{cod}")
                else:
                    chaves_oc_card.add(f"desc:{desc}")
            dups_placa_card = _computar_duplicidades_placa(
                coleta.oc.placa_normalizada, data_d1.isoformat(), chaves_oc_card,
            )

            # Forma de pagamento canônica: card do Pipefy (oficial) com
            # fallback para a forma do Club (prazo de pagamento).
            forma_canon = (
                (card.forma_pagamento if card and card.forma_pagamento else None)
                or coleta.oc.forma
            )

            resultados.append(
                ResultadoValidacao(
                    oc=coleta.oc,
                    status=status,
                    divergencias=divergencias,
                    fase_destino=fase,
                    valor_card=card.valor_card if card else None,
                    valor_club=coleta.oc.valor_pedido,
                    valor_pdf=card.valor_extraido_pdf if card else None,
                    valor_cilia=coleta.orcamento_cilia.valor_total
                    if coleta.orcamento_cilia
                    else None,
                    qtd_cotacoes=len(coleta.concorrentes),
                    qtd_produtos=len(coleta.produtos_cotacao),
                    peca_duplicada=peca_dup,
                    card_pipefy_id=card.id if card else None,
                    fase_pipefy_atual=fase_atual_nome,
                    reincidencia=reinc_card,
                    cancelamento=cancel_label,
                    cancelamento_card_id=cancel_card_id,
                    divergencias_cross=divs_cross_card,
                    produtos=coleta.produtos_cotacao,
                    chaves_reincidentes=chaves_reinc,
                    forma_pagamento_canonica=forma_canon,
                    duplicidades_placa=dups_placa_card,
                )
            )

        # 5. Contadores + persistência
        aprovadas = sum(1 for r in resultados if r.status == StatusValidacao.APROVADA)
        divergentes = sum(1 for r in resultados if r.status == StatusValidacao.DIVERGENCIA)
        bloqueadas = sum(1 for r in resultados if r.status == StatusValidacao.BLOQUEADA)
        aguardando_ml = sum(1 for r in resultados if r.status == StatusValidacao.AGUARDANDO_ML)
        ja_processadas = sum(1 for r in resultados if r.status == StatusValidacao.JA_PROCESSADA)

        validacao_id = registrar_validacao(
            data_d1=data_d1.isoformat(),
            total_ocs=len(resultados),
            aprovadas=aprovadas,
            divergentes=divergentes,
            bloqueadas=bloqueadas,
            dry_run=dry_run,
            executado_por=settings.validador_identificador,
            aguardando_ml=aguardando_ml,
            ja_processadas=ja_processadas,
        )

        for r in resultados:
            registrar_oc_resultado(
                validacao_id,
                {
                    "id_pedido": r.oc.id_pedido,
                    "id_cotacao": r.oc.id_cotacao,
                    "placa": r.oc.identificador,
                    "placa_normalizada": r.oc.placa_normalizada,
                    "fornecedor": r.oc.fornecedor.for_nome if r.oc.fornecedor else None,
                    "comprador": r.oc.comprador_nome
                    or (f"club_user:{r.oc.created_by}" if r.oc.created_by else None),
                    "forma_pagamento": r.oc.forma,
                    "valor_card": float(r.valor_card) if r.valor_card else None,
                    "valor_club": float(r.valor_club) if r.valor_club else None,
                    "valor_pdf": float(r.valor_pdf) if r.valor_pdf else None,
                    "valor_cilia": float(r.valor_cilia) if r.valor_cilia else None,
                    "qtd_cotacoes": r.qtd_cotacoes,
                    "qtd_produtos": r.qtd_produtos,
                    "peca_duplicada": r.peca_duplicada,
                    "status": r.status.value,
                    "regras_falhadas": [
                        {"regra": d.regra, "titulo": d.titulo}
                        for d in r.divergencias
                    ],
                    "fase_pipefy": r.fase_destino.value if r.fase_destino else None,
                    "card_pipefy_id": r.card_pipefy_id,
                    "fase_pipefy_atual": r.fase_pipefy_atual,
                },
            )

        # 6. Atuar no Pipefy.
        # Em modo "consulta" (default), nenhuma chamada externa é feita —
        # apenas registramos as ações planejadas em `acoes_pipefy_planejadas`.
        # Em modo "automatico", as mutations são realmente executadas.
        # `dry_run` ainda funciona como override por execução: se True,
        # nem o registro de ações planejadas acontece (modo "olhar e sair").
        if dry_run:
            logger.info("Dry run — Pipefy não foi alterado e nada foi registrado.")
        else:
            logger.info(
                "Atuando no Pipefy (modo=%s) para %d resultados...",
                settings.modo_operacao, len(resultados),
            )
            for r in resultados:
                try:
                    await _atuar_no_pipefy(pipefy, r, validacao_id)
                except Exception as e:
                    logger.error("Falha ao atuar no Pipefy para %s: %s", r.oc.id_pedido, e)

        logger.info(
            "=== Fim validação: %d OCs — aprovadas=%d divergentes=%d "
            "bloqueadas=%d ml=%d ja_processadas=%d ===",
            len(resultados), aprovadas, divergentes, bloqueadas,
            aguardando_ml, ja_processadas,
        )

        await cilia.close()
        return validacao_id, resultados, ocs_orfas
