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
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import HTTPException

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
from app.utils.chave_produto import chave_produto, chave_produto_de_obj
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

    Usa a função canônica `chave_produto_de_obj` de utils.chave_produto
    para garantir consistência com R2 e histórico.
    """
    if not produtos:
        return "—"
    from collections import defaultdict
    grupos: dict[str, int] = defaultdict(int)
    for p in produtos:
        chave = chave_produto_de_obj(p)
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


async def _buscar_historico_placa_pipefy(
    placa_normalizada: str,
    indice_cards: dict[str, list[CardPipefy]],
    club: ClubClient,
    *,
    data_max: date,
    dias_max: int,
    id_pedido_atual: str,
    pipefy: PipefyClient,
) -> list[dict[str, Any]]:
    """Busca histórico da placa via Pipefy + Club (substitui o backfill
    do Club para a R2 cross-time).

    Fluxo:
      1. Busca cards históricos da placa no índice pré-computado.
      2. Exclui o card atual (a OC não pode se comparar consigo mesma).
      3. Para cada card histórico, busca detalhes no Club (items).
      4. Retorna lista de dicts no formato compatível com
         `carregar_historico_bulk` (mesma shape que a tabela
         `historico_produtos_oc`), para que `detectar_reincidencias`
         possa indexar por `chave_produto`.

    Performance: faz 1 chamada ao Club por card histórico (não há API
    batch). Tipicamente 0-10 cards por placa na janela de 210 dias.
    """
    if not placa_normalizada:
        return []

    cards_historicos = await pipefy.buscar_cards_por_placa(
        placa_normalizada,
        indice=indice_cards,
        dias_maximo=dias_max,
    )

    # Excluir cards sem codigo_oc (nao servem para buscar no Club) e
    # excluir o card que representa a OC atual (nao comparar consigo mesma)
    id_atual_norm = str(id_pedido_atual or "").strip()
    cards_historicos = [
        c for c in cards_historicos
        if c.codigo_oc and c.codigo_oc.strip() and c.codigo_oc.strip() != id_atual_norm
    ]

    if not cards_historicos:
        return []

    async def _detalhes_do_card(card: CardPipefy) -> tuple[CardPipefy, dict[str, Any] | None]:
        # `.strip()` e critico: ha cards no Pipefy com codigo_oc contendo
        # espaco no comeco (ex: " 2040523"), provavelmente digitacao manual.
        # Sem strip, a URL do Club vira /orders/ 2040523 e retorna 404,
        # descartando silenciosamente o card do historico R2.
        codigo = (card.codigo_oc or "").strip()
        if not codigo:
            return card, None
        try:
            det = await club.get_order_details(codigo)
        except Exception as e:
            logger.warning(
                "Historico Pipefy: falha get_order_details OC %s (card %s): %s",
                codigo, card.id, e,
            )
            return card, None
        return card, det

    detalhes_por_card = await asyncio.gather(
        *[_detalhes_do_card(c) for c in cards_historicos]
    )

    items_historicos: list[dict[str, Any]] = []
    for card, det in detalhes_por_card:
        if not det:
            continue
        # Nota: NAO filtrar por status da OC historica. OCs canceladas
        # tambem devem contar como reincidencia (uma peca recomprada apos
        # cancelamento e exatamente o que o R2 cross-time precisa sinalizar).
        # Quando havia o filtro `status != "P"`, historicos com mix de
        # status apareciam silenciosamente vazios.

        forn = det.get("fornecedor") or {}
        forn_id = str(
            forn.get("for_id")
            or det.get("for_id")
            or ""
        ) or None
        forn_nome = forn.get("for_nome") or det.get("fornecedor_nome")

        data_oc_iso = ""
        if card.created_at:
            try:
                data_oc_iso = card.created_at.date().isoformat()
            except Exception:
                data_oc_iso = ""

        for item in det.get("items") or []:
            product = item.get("product") or {}
            desc_raw = product.get("name") or item.get("descricao") or ""
            chave = chave_produto(
                ean=product.get("ean"),
                codigo=product.get("internal_code"),
                descricao=desc_raw,
            )
            # Normalized description for secondary indexing fallback —
            # even if EAN/code is inconsistent, description matching works.
            desc_normalizada = desc_raw.strip().lower() if desc_raw else ""
            items_historicos.append({
                "id_pedido": str(card.codigo_oc),
                "id_cotacao": str(det.get("id_cotacao") or "") or None,
                "data_oc": data_oc_iso,
                "identificador": placa_normalizada,
                "placa_normalizada": placa_normalizada,
                "chave_produto": chave,
                "descricao": desc_raw,
                "descricao_normalizada": desc_normalizada,
                "fornecedor_id": forn_id,
                "fornecedor_nome": forn_nome,
                "quantidade": float(item.get("quantity") or 0),
                "card_pipefy_id": card.id,
            })

    return items_historicos


def _buscar_historico_placa_club(
    placa_normalizada: str,
    todos_pedidos_historicos: dict[str, list[dict[str, Any]]],
    *,
    id_pedido_atual: str,
) -> list[dict[str, Any]]:
    """Busca historico da placa diretamente nos pedidos pre-fetched do Club API.

    Complementa `_buscar_historico_placa_pipefy` — muitos cards antigos no
    Pipefy nao possuem `codigo_oc` preenchido, fazendo com que a fonte
    Pipefy perca OCs historicas. O Club API, por outro lado, SEMPRE tem
    todas as OCs por data.

    Parametros:
      placa_normalizada: placa normalizada (sem hifen, upper).
      todos_pedidos_historicos: dict pre-construido de
          placa_normalizada -> list[pedido_v3_normalizado], contendo TODOS
          os pedidos do Club na janela historica (excluindo D-1).
      id_pedido_atual: id_pedido da OC sendo validada (para exclusao).

    Retorna items no mesmo formato que `_buscar_historico_placa_pipefy`.
    """
    if not placa_normalizada:
        return []

    pedidos = todos_pedidos_historicos.get(placa_normalizada, [])
    if not pedidos:
        return []

    id_atual_norm = str(id_pedido_atual or "").strip()
    items_historicos: list[dict[str, Any]] = []

    for pedido in pedidos:
        id_pedido = str(pedido.get("id_pedido") or pedido.get("id") or "").strip()
        if id_pedido == id_atual_norm:
            continue

        forn_raw = pedido.get("fornecedor") or {}
        forn_id = str(
            forn_raw.get("for_id")
            or pedido.get("for_id")
            or ""
        ) or None
        forn_nome = forn_raw.get("for_nome") or pedido.get("fornecedor_nome")

        data_oc_raw = pedido.get("data_pedido") or pedido.get("generation_date") or ""
        data_oc_iso = ""
        if data_oc_raw:
            parsed = _parse_data(data_oc_raw)
            if parsed:
                data_oc_iso = parsed.isoformat()

        for item in pedido.get("items") or pedido.get("itens") or pedido.get("products") or []:
            product = item.get("product") or {}
            desc_raw = product.get("name") or item.get("descricao") or ""
            chave = chave_produto(
                ean=product.get("ean"),
                codigo=product.get("internal_code"),
                descricao=desc_raw,
            )
            desc_normalizada = desc_raw.strip().lower() if desc_raw else ""
            items_historicos.append({
                "id_pedido": id_pedido,
                "id_cotacao": str(pedido.get("id_cotacao") or "") or None,
                "data_oc": data_oc_iso,
                "identificador": placa_normalizada,
                "placa_normalizada": placa_normalizada,
                "chave_produto": chave,
                "descricao": desc_raw,
                "descricao_normalizada": desc_normalizada,
                "fornecedor_id": forn_id,
                "fornecedor_nome": forn_nome,
                "quantidade": float(item.get("quantity") or item.get("quantidade") or 0),
                "card_pipefy_id": None,  # Club source — no Pipefy card
            })

    return items_historicos


def _indexar_historico_por_chave(
    items: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Transforma a lista de items do histórico Pipefy/Club em um dict
    chave_produto -> [registros], compatível com o retorno de
    `carregar_historico_bulk` e com o parâmetro `_historico_bulk` de
    `detectar_reincidencias`.

    A ordenação (mais recente primeiro) é preservada porque a lista de
    entrada já vem ordenada por `created_at` DESC do pipefy_client.

    SECONDARY INDEXING: além da chave primária (ean:X / cod:X / desc:X),
    cada item é TAMBÉM indexado sob "desc:<descricao_normalizada>" quando a
    chave primária não é baseada em descrição. Isso permite matching por
    descrição como fallback — útil quando EAN/código é inconsistente entre
    fontes (ex: Pipefy vs Club vs Cilia).
    """
    out: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        chave = item.get("chave_produto")
        if not chave:
            continue
        out.setdefault(chave, []).append(item)

        # Secondary index by normalized description when primary key
        # is NOT already desc-based (avoid double-indexing)
        if not chave.startswith("desc:"):
            desc_norm = item.get("descricao_normalizada") or ""
            if not desc_norm:
                # Fallback: compute from raw description
                desc_raw = item.get("descricao") or ""
                desc_norm = desc_raw.strip().lower() if desc_raw else ""
            if desc_norm:
                chave_desc = f"desc:{desc_norm}"
                out.setdefault(chave_desc, []).append(item)
    return out


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
            valor_unitario=_to_decimal(it.get("unit_price") or it.get("valor_unitario")),
            valor_total=_to_decimal(it.get("total_price") or it.get("valor_total")),
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

    # Mapeamento chave interna → label do campo no Pipefy (para comparar
    # valor atual do card e evitar mutations desnecessárias).
    _CAMPO_LABEL: dict[str, str] = {
        "peca_duplicada": "Peça duplicada?",
        "abatimento_fornecedor": "Abatimento fornecedor?",
        "validacao_concluida_por": "Validação da Oc concluída por:",
        "validacao_concluida": "Validação concluída?",
        "justificativa_divergencia": "Informe a negativa da validação",
    }

    def _campo_ja_igual(campo_chave: str, valor_novo: str) -> bool:
        """Retorna True se o valor atual do card já é igual ao desejado."""
        label = _CAMPO_LABEL.get(campo_chave)
        if not label or not resultado.card_campos:
            return False
        valor_atual = str(resultado.card_campos.get(label) or "").strip()
        return valor_atual == str(valor_novo).strip()

    # Lista de ações planejadas: (acao, payload_dict, motivo)
    acoes: list[tuple[str, dict[str, Any], str | None]] = []
    campos_skipped = 0

    for campo, valor in [
        ("peca_duplicada", resultado.peca_duplicada),
        ("abatimento_fornecedor", resultado.abatimento_fornecedor),
        ("validacao_concluida_por", settings.validador_identificador),
        ("validacao_concluida", "Sim" if resultado.aprovada else "Não"),
    ]:
        if _campo_ja_igual(campo, valor):
            campos_skipped += 1
        else:
            acoes.append(("update_field", {"campo": campo, "valor": valor}, None))

    if resultado.divergencias:
        texto = "\n".join(
            f"[{d.regra}] {d.titulo}: {d.descricao}"
            for d in resultado.divergencias
        )
        if not _campo_ja_igual("justificativa_divergencia", texto):
            acoes.append((
                "update_field",
                {"campo": "justificativa_divergencia", "valor": texto},
                "registrar divergências no card",
            ))
        else:
            campos_skipped += 1

    if campos_skipped:
        logger.info(
            "Card %s: %d campo(s) já com valor correto — skip mutation",
            resultado.card_pipefy_id, campos_skipped,
        )

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
    concorrencia: int = 10,
) -> tuple[int, list[ResultadoValidacao], list[OcOrfa], dict | None]:
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

    Retorna (validacao_id, resultados, ocs_orfas, historico_status).
    `historico_status` é o dict retornado por `garantir_historico` (ou
    None se `r2_modo == "off"`).
    """
    try:
        async with asyncio.timeout(900):  # 15 minutos maximo
            return await _executar_validacao_impl(data_d1, dry_run=dry_run, concorrencia=concorrencia)
    except asyncio.TimeoutError:
        logger.error("Timeout global na validacao de %s", data_d1)
        raise HTTPException(504, "Validacao excedeu o tempo limite de 15 minutos")


async def _executar_validacao_impl(
    data_d1: date,
    *,
    dry_run: bool = True,
    concorrencia: int = 10,
) -> tuple[int, list[ResultadoValidacao], list[OcOrfa], dict | None]:
    """Implementacao interna do pipeline (chamada por executar_validacao com timeout)."""
    init_db()

    # Backup preventivo antes de operacoes criticas
    from app.db import backup_db
    try:
        backup_db()
    except Exception as e:
        logger.warning("Falha ao criar backup preventivo: %s", e)

    logger.info(
        "=== Inicio validacao D-1=%s dry_run=%s ===", data_d1, dry_run
    )

    cilia = build_cilia_client()

    async with ClubClient() as club, PipefyClient(dry_run=dry_run) as pipefy:
        # 1. Indexar OCs do Club do D-1 por id_pedido E por placa.
        # O indice por placa e usado como FALLBACK em 4 (abaixo) quando
        # um card do Pipefy tem codigo_oc vazio/invalido — sem esse
        # fallback a OC vira uma entrada sintetica sem cotacao no
        # dashboard e a mesma placa reaparece na Revisao Final com
        # cotacao (bug observado com QQF2C69 em 2026-04-10).
        pedidos_raw = await club.listar_pedidos(data_d1)
        logger.info("Club: %d OCs encontradas em %s", len(pedidos_raw), data_d1)
        ocs_index: dict[str, dict[str, Any]] = {}
        ocs_por_placa: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for raw in pedidos_raw:
            id_pedido = str(raw.get("id_pedido") or raw.get("id") or "").strip()
            if not id_pedido:
                continue
            ocs_index[id_pedido] = raw
            placa_raw = raw.get("identificador") or raw.get("identifier") or ""
            placa_norm = PipefyClient._normalizar_placa(str(placa_raw))
            if placa_norm:
                ocs_por_placa.setdefault(placa_norm, []).append((id_pedido, raw))

        # 2a. Listar fases de cancelamento ANTES de listar_todos para
        # evitar buscar essas fases duas vezes (economiza tokens Pipefy).
        # Os cards brutos são reaproveitados no índice R2 logo abaixo.
        _canc_cache_dicts: list[dict[str, Any]] = []
        _canc_raw_cards: list[CardPipefy] = []
        _skip_fases: set[str] = set()
        if settings.r2_modo != "off":
            try:
                result = await pipefy.listar_cards_cancelamento_pipe_principal(
                    return_raw_cards=True,
                )
                _canc_cache_dicts, _canc_raw_cards = result  # type: ignore[misc]
                atualizar_cache_cancelamentos(_canc_cache_dicts)
                # Pular estas fases em listar_todos — já temos os cards
                _skip_fases = set(settings.fases_cancelamento_list)
            except Exception as e:
                logger.error("Falha ao listar cancelamentos (pre-fetch): %s", e)

        # 2b. Listar cards das DEMAIS fases do pipe principal (historico
        # para R2 cross-time + base para filtrar cards do D-1).
        # As fases de cancelamento são puladas se já listadas acima.
        todos_cards: list[CardPipefy] = (
            await pipefy.listar_todos_cards_pipe_principal(
                skip_fases=_skip_fases or None,
            )
        )

        # 2c. Merge: incorporar cards de cancelamento no índice histórico
        # para manter a cobertura R2 completa (sem perder nenhum card).
        if _canc_raw_cards:
            vistos_ids = {c.id for c in todos_cards}
            merged = 0
            for c in _canc_raw_cards:
                if c.id not in vistos_ids:
                    todos_cards.append(c)
                    vistos_ids.add(c.id)
                    merged += 1
            if merged:
                logger.info(
                    "Pipefy: %d cards de cancelamento mergeados no índice histórico",
                    merged,
                )
        elif not _canc_cache_dicts and not _skip_fases:
            # Fallback: pre-fetch de cancelamentos falhou, mas listar_todos
            # listou todas as 12 fases. Extrair cards de cancelamento de
            # todos_cards para atualizar o cache (evita cache stale).
            fases_cancel = set(settings.fases_cancelamento_list)
            canc_fallback: list[dict[str, Any]] = []
            for c in todos_cards:
                if c.phase_name and c.phase_name in fases_cancel:
                    placa_norm = (
                        (c.title or "").replace("-", "").replace(" ", "").upper()
                    )
                    if placa_norm:
                        tipo = (
                            "informacoes_incorretas"
                            if c.phase_name == "Informações Incorretas"
                            else "cancelado"
                        )
                        canc_fallback.append({
                            "placa_normalizada": placa_norm,
                            "card_id": str(c.id),
                            "tipo": tipo,
                            "fase_atual": c.phase_name,
                            "descricao_pecas": c.descricao_pecas,
                            "codigo_oc": c.codigo_oc,
                        })
            if canc_fallback:
                atualizar_cache_cancelamentos(canc_fallback)
                logger.info(
                    "Cache cancelamentos atualizado via fallback: %d cards",
                    len(canc_fallback),
                )

        logger.info(
            "Pipefy: %d cards historicos lidos (pipe principal)",
            len(todos_cards),
        )

        # 2d. Construir indice placa_normalizada -> [CardPipefy] UMA vez.
        # Sera reusado por `_buscar_historico_placa_pipefy` para cada OC
        # sendo validada, eliminando queries repetidas ao Pipefy.
        indice_cards_historicos = pipefy.indexar_cards_por_placa(todos_cards)
        logger.info(
            "Pipefy: indice historico com %d placas distintas",
            len(indice_cards_historicos),
        )

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

        # 4. Matching card ↔ OC em 2 FASES SEQUENCIAIS.
        #
        # Antes, o matching direto (por codigo_oc) e o fallback (por placa)
        # rodavam juntos dentro de asyncio.gather com `ids_pedido_consumidos`
        # compartilhado — race condition: um card que precisa de fallback
        # podia consumir uma OC antes do card que a reivindica pelo codigo_oc
        # direto, "roubando" a OC e deixando a outra OC orfa.
        #
        # Agora:
        #  Fase 1 (sincrona): processa todos os cards com codigo_oc VALIDO
        #    no ocs_index E cuja placa bate com a da OC (protecao contra
        #    cross-matching de placas). Marca ids como consumidos.
        #  Fase 2 (sincrona): para cada card que sobrou, tenta fallback
        #    por placa (ocs_por_placa), respeitando ids ja consumidos.
        #  Fase 3 (paralela): roda `_coletar_para_card` em paralelo com
        #    semaforo — agora seguro, porque raw ja esta resolvido.
        semaforo = asyncio.Semaphore(concorrencia)
        ids_pedido_consumidos: set[str] = set()
        matches: list[tuple[CardPipefy, dict[str, Any] | None]] = []

        # --- Fase 1: matching direto por codigo_oc (com validacao de placa)
        cards_sem_match_direto: list[CardPipefy] = []
        for card in cards_do_dia:
            raw = None
            id_matched: str | None = None
            if card.codigo_oc:
                codigo_norm = card.codigo_oc.strip()
                candidate = ocs_index.get(codigo_norm)
                if candidate is not None:
                    # Validar que a placa da OC bate com a do card — protege
                    # contra cards com codigo_oc apontando para OC de placa
                    # errada (digitacao manual).
                    placa_card_norm = PipefyClient._normalizar_placa(
                        card.title or ""
                    )
                    placa_oc_raw = (
                        candidate.get("identificador")
                        or candidate.get("identifier")
                        or ""
                    )
                    placa_oc_norm = PipefyClient._normalizar_placa(
                        str(placa_oc_raw)
                    )
                    if (
                        not placa_card_norm
                        or not placa_oc_norm
                        or placa_card_norm == placa_oc_norm
                    ):
                        raw = candidate
                        id_matched = codigo_norm
                    else:
                        logger.warning(
                            "Matching direto REJEITADO: card %s placa=%s "
                            "aponta para OC %s placa=%s — caira em fallback",
                            card.id, placa_card_norm,
                            codigo_norm, placa_oc_norm,
                        )
            if raw is not None and id_matched is not None:
                ids_pedido_consumidos.add(id_matched)
                matches.append((card, raw))
            else:
                cards_sem_match_direto.append(card)

        # --- Fase 1.5: descartar cards cujo codigo_oc aponta para OC de
        # outro dia no Club. Cenario: comprador gera OC no Club em DD-N
        # mas so cria o card no Pipefy em D-1 (lancamento atrasado de N
        # dias). Esses cards NAO devem entrar no batch atual — ja foram
        # (ou serao) tratados na rodada da data correta. O sistema gera
        # log de auditoria para o analista rodar `validar --data DD-N`.
        # Conversao de timezone: o Club retorna `data_pedido` em UTC; converter
        # para America/Sao_Paulo antes de comparar com data_d1 (data local).
        from zoneinfo import ZoneInfo
        _TZ_SP = ZoneInfo("America/Sao_Paulo")

        def _parse_data_sp(v: Any) -> date | None:
            """Igual a _parse_data, mas converte timezone aware p/ SP antes
            do .date(). Evita descarte indevido de OCs do D-1 noite."""
            if not v:
                return None
            if isinstance(v, date) and not isinstance(v, datetime):
                return v
            s = str(v).strip()
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is not None:
                    dt = dt.astimezone(_TZ_SP)
                return dt.date()
            except ValueError:
                return _parse_data(v)

        # Sentinela para distinguir "falha de rede" de "sem codigo_oc".
        # date.min nao pode aparecer naturalmente em data_pedido — uso seguro.
        _FAIL = date.min

        cards_descartados_outro_dia: list[tuple[CardPipefy, date]] = []
        cards_falha_lookup: list[CardPipefy] = []
        cards_para_fallback: list[CardPipefy] = []

        async def _data_oc_no_club(
            card: CardPipefy,
        ) -> tuple[CardPipefy, date | None]:
            codigo = (card.codigo_oc or "").strip()
            if not codigo:
                return card, None
            async with semaforo:
                try:
                    det = await club.get_order_details(codigo)
                except Exception as e:
                    logger.warning(
                        "Fase 1.5: falha get_order_details OC %s "
                        "(card %s, placa=%s): %s — card MANTIDO no "
                        "fallback (nao descartado por seguranca)",
                        codigo, card.id, card.title, e,
                    )
                    return card, _FAIL
            data_raw = (
                det.get("data_pedido") or det.get("generation_date") or ""
            )
            return card, _parse_data_sp(data_raw)

        cards_com_codigo = [
            c for c in cards_sem_match_direto
            if (c.codigo_oc or "").strip()
        ]
        cards_sem_codigo = [
            c for c in cards_sem_match_direto
            if not (c.codigo_oc or "").strip()
        ]
        if cards_com_codigo:
            datas_oc = await asyncio.gather(
                *[_data_oc_no_club(c) for c in cards_com_codigo]
            )
            for card, data_oc in datas_oc:
                if data_oc is _FAIL:
                    # Falha de rede — manter no fallback (fail-safe)
                    cards_falha_lookup.append(card)
                    cards_para_fallback.append(card)
                elif data_oc and data_oc != data_d1:
                    cards_descartados_outro_dia.append((card, data_oc))
                    logger.warning(
                        "Card %s (placa=%s, codigo_oc=%s) DESCARTADO do "
                        "batch de %s: OC referenciada e do dia %s. "
                        "Lancamento atrasado — rodar `validar --data %s` "
                        "para validar OCs daquele dia.",
                        card.id, card.title, card.codigo_oc,
                        data_d1, data_oc, data_oc,
                    )
                else:
                    cards_para_fallback.append(card)
        cards_para_fallback.extend(cards_sem_codigo)

        if cards_descartados_outro_dia:
            datas_unicas = sorted({d.isoformat() for _, d in cards_descartados_outro_dia})
            logger.info(
                "Fase 1.5: %d cards descartados (lancamento atrasado) — "
                "datas das OCs referenciadas: %s",
                len(cards_descartados_outro_dia), datas_unicas,
            )
        if cards_falha_lookup:
            logger.warning(
                "Fase 1.5: %d cards com falha no lookup get_order_details — "
                "mantidos no fallback (fail-safe). Verificar logs detalhados.",
                len(cards_falha_lookup),
            )

        # --- Fase 2: fallback por placa para os cards restantes
        for card in cards_para_fallback:
            raw = None
            placa_card_norm = PipefyClient._normalizar_placa(card.title or "")
            if placa_card_norm:
                candidatos = ocs_por_placa.get(placa_card_norm, [])
                for candidate_id, candidate_raw in candidatos:
                    if candidate_id in ids_pedido_consumidos:
                        continue
                    raw = candidate_raw
                    ids_pedido_consumidos.add(candidate_id)
                    logger.info(
                        "Fallback por placa: card %s (placa %s, "
                        "codigo_oc=%r) matched com OC %s do Club",
                        card.id, placa_card_norm,
                        card.codigo_oc, candidate_id,
                    )
                    break
            matches.append((card, raw))

        # --- Fase 3: coleta paralela (semaforo) com raws ja resolvidos
        async def _coleta(card: CardPipefy, raw: dict[str, Any] | None):
            async with semaforo:
                return await _coletar_para_card(card, raw, club, cilia, pipefy)

        coletas = await asyncio.gather(
            *[_coleta(c, r) for c, r in matches]
        )

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
        #
        # Dedupe defensivo por placa: se uma placa ja tem card em `coletas`
        # (inclusive card orfao com codigo_oc vazio, via _oc_minima_para_card_orfao),
        # NAO pode aparecer tambem na lista de orfas. Sem isso, a mesma placa
        # pode aparecer 2x no relatorio (dashboard com card sem cotacao +
        # Revisao Final com cotacao) — bug observado com a placa QQF2C69.
        placas_com_card: set[str] = {
            c.oc.placa_normalizada for c in coletas if c.oc.placa_normalizada
        }
        orfas_raw: list[tuple[OrdemCompra, str | None]] = []
        for id_pedido, raw in ocs_index.items():
            if id_pedido in ids_pedido_consumidos:
                continue
            oc = _parse_oc(raw)
            if oc.placa_normalizada and oc.placa_normalizada in placas_com_card:
                logger.info(
                    "Dedupe: OC %s (placa %s) ja tem card em resultados, "
                    "nao adiciona a Revisao Final",
                    oc.id_pedido, oc.placa_normalizada,
                )
                continue
            comprador = None
            if oc.created_by:
                nome, _ = compradores_svc.resolve(oc.created_by)
                comprador = nome
            orfas_raw.append((oc, comprador))

        async def _dados_orfa(
            oc: OrdemCompra,
        ) -> tuple[list[ProdutoCotacao], int]:
            """Busca os ITENS efetivamente comprados na OC órfã via
            `get_order_details(id_pedido).items` (NÃO `get_produtos_cotacao`,
            que retorna a cotação inteira e gera falsos positivos quando
            várias OCs compartilham a mesma cotação).
            Também busca concorrentes via `get_concorrentes(id_cotacao)`
            para preencher qtd_cotacoes.
            Retorna (produtos, qtd_concorrentes)."""
            produtos: list[ProdutoCotacao] = []
            qtd_conc = 0
            async with semaforo:
                try:
                    det = await club.get_order_details(oc.id_pedido)
                except Exception as e:
                    logger.warning(
                        "Falha get_order_details para OC órfã %s: %s",
                        oc.id_pedido, e,
                    )
                    return [], 0
                items = det.get("items") or []
                produtos = [
                    ProdutoCotacao(
                        produto_id=str(
                            (it.get("product") or {}).get("id") or ""
                        ),
                        descricao=(it.get("product") or {}).get("name"),
                        quantidade=float(it.get("quantity") or 0),
                        ean=(it.get("product") or {}).get("ean"),
                        cod_interno=(it.get("product") or {}).get("internal_code"),
                        valor_unitario=_to_decimal(it.get("unit_price") or it.get("valor_unitario")),
                        valor_total=_to_decimal(it.get("total_price") or it.get("valor_total")),
                    )
                    for it in items
                ]
                # Buscar concorrentes se tiver id_cotacao
                if oc.id_cotacao:
                    try:
                        concs = await club.get_concorrentes(oc.id_cotacao)
                        qtd_conc = len(concs)
                    except Exception as e:
                        logger.warning(
                            "Falha get_concorrentes para OC órfã %s (cot %s): %s",
                            oc.id_pedido, oc.id_cotacao, e,
                        )
            return produtos, qtd_conc

        dados_orfas = await asyncio.gather(
            *[_dados_orfa(oc) for oc, _ in orfas_raw]
        )
        produtos_por_orfa = [d[0] for d in dados_orfas]
        cotacoes_por_orfa = [d[1] for d in dados_orfas]
        logger.info(
            "Coletados produtos para %d OCs órfãs", len(orfas_raw)
        )

        # 4d. R2 cross-time — preparar histórico e cache de devoluções
        # antes de aplicar as regras.
        #
        # Fonte do histórico é controlada por `settings.r2_fonte_historico`:
        #   - "pipefy" (default): historico vem do indice de cards do pipe
        #                         principal + detalhes sob demanda no Club.
        #                         NADA a preparar aqui — o helper
        #                         `_buscar_historico_placa_pipefy` é chamado
        #                         por OC dentro do loop mais abaixo.
        #   - "sqlite" (legado):  backfill incremental via `garantir_historico`.
        #
        # IMPORTANTE (modo sqlite): NÃO silenciamos exceções do
        # `garantir_historico`. Se o backfill falhar por erro crítico
        # (DB schema, network fatal, etc.), a validação DEVE falhar.
        historico_status: dict | None = None
        if settings.r2_modo != "off" and settings.r2_fonte_historico == "sqlite":
            # time_budget de 5min para o backfill dentro da validacao normal
            # (backfill longo deve usar o endpoint /api/admin/backfill)
            historico_status = await garantir_historico(
                club,
                ate_dia=data_d1,
                dias_janela=settings.r2_janela_dias,
                time_budget_seconds=300.0,
            )
            if not historico_status["completo"]:
                logger.warning(
                    "HISTORICO INCOMPLETO: %d/%d dias cobertos. "
                    "Alertas de duplicidade podem estar incompletos.",
                    historico_status["dias_cobertos"],
                    historico_status["dias_necessarios"],
                )
            else:
                logger.info(
                    "Histórico completo: %d dias",
                    historico_status["dias_cobertos"],
                )
        elif settings.r2_modo != "off":
            logger.info(
                "R2 cross-time: fonte do historico = pipefy (indice com %d placas)",
                len(indice_cards_historicos),
            )

        # Persistencia do D-1 no `historico_produtos_oc` — so faz sentido
        # no modo sqlite (o modo pipefy nao le dessa tabela).
        if settings.r2_modo != "off" and settings.r2_fonte_historico == "sqlite":
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

        # Cache de devoluções — sempre precisa rodar para enriquecer divergências.
        # (Cache de cancelamentos já foi atualizado no passo 2a acima.)
        if settings.r2_modo != "off":
            try:
                devs = await pipefy.listar_devolucoes_abertas()
                atualizar_cache_devolucoes(devs)
            except Exception as e:
                logger.error("Falha ao atualizar cache de devoluções: %s", e)

        # 4d.1 — Pre-calcular o historico indexado via Pipefy PARA CADA
        # placa que sera validada (cards do dia + orfãs). Carregamos a
        # lista completa (sem excluir nenhum id_pedido) UMA vez por placa,
        # e o filtro "exclui a OC atual" e aplicado depois, na hora de
        # montar o contexto de cada OC individual.
        historico_items_por_placa: dict[str, list[dict[str, Any]]] = {}

        # COMPLEMENTARY SOURCE: fetch ALL historical OCs from the Club API
        # for the R2 window period. Many old Pipefy cards lack `codigo_oc`,
        # so the Pipefy-only source misses them. The Club API always has
        # every OC by date, making it the authoritative complement.
        club_historico_por_placa: dict[str, list[dict[str, Any]]] = {}
        if (
            settings.r2_modo != "off"
            and settings.r2_fonte_historico == "pipefy"
        ):
            janela_inicio = data_d1 - timedelta(days=settings.r2_janela_dias)
            janela_fim = data_d1 - timedelta(days=1)
            try:
                pedidos_historicos_club = await club.listar_pedidos_v3(
                    janela_inicio,
                    janela_fim,
                    products=True,
                    seller=True,
                )
                # Diagnóstico: quantos pedidos têm items inline?
                com_items = sum(
                    1 for p in pedidos_historicos_club
                    if p.get("items") or p.get("itens")
                )
                sem_items_list = [
                    p for p in pedidos_historicos_club
                    if not (p.get("items") or p.get("itens"))
                ]
                logger.info(
                    "Club v3: %d OCs historicas carregadas de %s a %s "
                    "para complemento R2 (%d com items, %d sem items)",
                    len(pedidos_historicos_club), janela_inicio, janela_fim,
                    com_items, len(sem_items_list),
                )

                # Enriquecer pedidos sem items via get_order_details
                if sem_items_list:
                    logger.info(
                        "Enriquecendo %d pedidos sem items via "
                        "get_order_details...",
                        len(sem_items_list),
                    )
                    sem_enrich = asyncio.Semaphore(concorrencia)

                    async def _enriquecer_pedido(pedido: dict):
                        id_p = str(
                            pedido.get("id_pedido")
                            or pedido.get("id")
                            or ""
                        ).strip()
                        if not id_p:
                            return
                        async with sem_enrich:
                            try:
                                det = await club.get_order_details(id_p)
                                pedido["items"] = (
                                    det.get("items")
                                    or det.get("itens")
                                    or []
                                )
                            except Exception as e:
                                logger.warning(
                                    "Falha enriquecer OC hist %s: %s",
                                    id_p, e,
                                )

                    await asyncio.gather(
                        *[_enriquecer_pedido(p) for p in sem_items_list]
                    )
                    enriquecidos = sum(
                        1 for p in sem_items_list
                        if p.get("items")
                    )
                    logger.info(
                        "Enriquecidos %d/%d pedidos com items",
                        enriquecidos, len(sem_items_list),
                    )

                # Index by placa_normalizada
                for pedido in pedidos_historicos_club:
                    placa_raw = (
                        pedido.get("identificador")
                        or pedido.get("identifier")
                        or ""
                    )
                    placa_norm = (
                        str(placa_raw)
                        .replace("-", "")
                        .replace(" ", "")
                        .upper()
                        .strip()
                    )
                    if placa_norm:
                        club_historico_por_placa.setdefault(
                            placa_norm, []
                        ).append(pedido)
            except Exception as e:
                logger.error(
                    "Falha ao carregar historico Club v3 para R2: %s", e,
                )
                # Continue with Pipefy-only history — degraded but functional

        if (
            settings.r2_modo != "off"
            and settings.r2_fonte_historico == "pipefy"
        ):
            placas_unicas: set[str] = set()
            for coleta in coletas:
                if coleta.oc.placa_normalizada:
                    placas_unicas.add(coleta.oc.placa_normalizada)
            for oc, _comprador in orfas_raw:
                if oc.placa_normalizada:
                    placas_unicas.add(oc.placa_normalizada)

            sem_hist = asyncio.Semaphore(concorrencia)

            async def _carregar_placa(placa: str):
                async with sem_hist:
                    items = await _buscar_historico_placa_pipefy(
                        placa,
                        indice_cards_historicos,
                        club,
                        data_max=data_d1,
                        dias_max=settings.r2_janela_dias,
                        id_pedido_atual="",  # sem exclusao aqui — feita por OC
                        pipefy=pipefy,
                    )
                return placa, items

            resultados_hist = await asyncio.gather(
                *[_carregar_placa(p) for p in placas_unicas]
            )
            for placa, items in resultados_hist:
                historico_items_por_placa[placa] = items
            logger.info(
                "Historico Pipefy pre-carregado para %d placas "
                "(%d items totais)",
                len(historico_items_por_placa),
                sum(len(v) for v in historico_items_por_placa.values()),
            )

            # MERGE Club history into Pipefy history — Club items
            # complement Pipefy items. Avoid duplicates by checking
            # (id_pedido, chave_produto) pairs already present.
            total_club_merged = 0
            for placa in placas_unicas:
                club_items = _buscar_historico_placa_club(
                    placa,
                    club_historico_por_placa,
                    id_pedido_atual="",  # exclusion by OC is done later
                )
                if not club_items:
                    continue
                # Build set of existing (id_pedido, chave_produto) from
                # Pipefy items to avoid duplicates
                existing = set()
                for it in historico_items_por_placa.get(placa, []):
                    existing.add((
                        str(it.get("id_pedido") or "").strip(),
                        it.get("chave_produto") or "",
                    ))
                novos = [
                    it for it in club_items
                    if (
                        str(it.get("id_pedido") or "").strip(),
                        it.get("chave_produto") or "",
                    ) not in existing
                ]
                if novos:
                    historico_items_por_placa.setdefault(placa, []).extend(novos)
                    total_club_merged += len(novos)
            if total_club_merged:
                logger.info(
                    "Club historico: %d items complementares mergeados "
                    "(%d items totais agora)",
                    total_club_merged,
                    sum(len(v) for v in historico_items_por_placa.values()),
                )

            # 3a FONTE DE HISTORICO: cache de devolucoes (pipe 305658860).
            # Cobre OCs antigas que: (a) ja sairam do pipe principal e
            # (b) nao tem placa indexada na API v3 do Club (request.obs vazio).
            # Para cada placa do batch, busca devolucoes locais; para cada n_oc,
            # carrega items reais via get_order_details (com EAN/codigo) — assim
            # a chave_produto bate com a OC atual e o R2 detecta a duplicidade.
            sem_dev = asyncio.Semaphore(concorrencia)

            async def _historico_via_devolucoes(
                placa: str,
            ) -> tuple[str, list[dict[str, Any]]]:
                """Retorna items do historico via cache_devolucoes,
                enriquecidos com get_order_details para ter EAN/codigo.
                Dedup por (id_pedido, chave_produto) — alinhado com o merge
                Club, permite complementar items quando a OC ja existe em
                outra fonte mas com subset de pecas diferente."""
                devs = get_devolucoes_por_placa(placa)
                if not devs:
                    return placa, []
                # Set (id_pedido, chave_produto) ja presente no historico
                ja_no_hist: set[tuple[str, str]] = {
                    (
                        str(it.get("id_pedido") or "").strip(),
                        it.get("chave_produto") or "",
                    )
                    for it in historico_items_por_placa.get(placa, [])
                }
                novos_items: list[dict[str, Any]] = []
                for d in devs:
                    n_oc = (d.get("n_oc") or "").strip()
                    if not n_oc:
                        continue
                    async with sem_dev:
                        try:
                            det = await club.get_order_details(n_oc)
                        except Exception as e:
                            logger.debug(
                                "cache_devolucoes: falha get_order_details "
                                "OC %s placa %s: %s", n_oc, placa, e,
                            )
                            continue
                    forn = det.get("fornecedor") or {}
                    forn_id = str(
                        forn.get("for_id") or det.get("for_id") or ""
                    ) or None
                    forn_nome = (
                        forn.get("for_nome")
                        or det.get("fornecedor_nome")
                    )
                    data_oc_iso = ""
                    data_raw = (
                        det.get("data_pedido")
                        or det.get("generation_date")
                        or ""
                    )
                    if data_raw:
                        parsed = _parse_data(data_raw)
                        if parsed:
                            data_oc_iso = parsed.isoformat()
                    for item in det.get("items") or []:
                        product = item.get("product") or {}
                        desc_raw = (
                            product.get("name")
                            or item.get("descricao")
                            or ""
                        )
                        chave = chave_produto(
                            ean=product.get("ean"),
                            codigo=product.get("internal_code"),
                            descricao=desc_raw,
                        )
                        # Dedup fino: pular item se essa OC ja contribuiu com
                        # essa peca no historico via outra fonte (Pipefy/Club)
                        if (n_oc, chave) in ja_no_hist:
                            continue
                        desc_norm = (
                            desc_raw.strip().lower() if desc_raw else ""
                        )
                        novos_items.append({
                            "id_pedido": n_oc,
                            "id_cotacao": str(
                                det.get("id_cotacao") or ""
                            ) or None,
                            "data_oc": data_oc_iso,
                            "identificador": placa,
                            "placa_normalizada": placa,
                            "chave_produto": chave,
                            "descricao": desc_raw,
                            "descricao_normalizada": desc_norm,
                            "fornecedor_id": forn_id,
                            "fornecedor_nome": forn_nome,
                            "quantidade": float(item.get("quantity") or 0),
                            "card_pipefy_id": d.get("card_id"),
                            "fonte_historico": "cache_devolucoes",
                        })
                return placa, novos_items

            try:
                resultados_dev = await asyncio.gather(
                    *[_historico_via_devolucoes(p) for p in placas_unicas]
                )
                total_dev_merged = 0
                for placa, items in resultados_dev:
                    if not items:
                        continue
                    historico_items_por_placa.setdefault(placa, []).extend(items)
                    total_dev_merged += len(items)
                if total_dev_merged:
                    logger.info(
                        "cache_devolucoes: %d items adicionados ao historico "
                        "R2 (cobre OCs sem card no pipe principal e sem "
                        "placa indexada no Club)",
                        total_dev_merged,
                    )
            except Exception as e:
                logger.error(
                    "Falha ao mergear cache_devolucoes no historico R2: %s",
                    e,
                )

        # 4d.2 — INJETAR items do D-1 atual no historico para detecção
        # intra-batch. Sem isso, OCs da MESMA placa no MESMO D-1 não se
        # veem (o Club historico só vai até D-2). O _historico_indexado_para_oc
        # exclui a própria OC depois, garantindo que A não se auto-detecte.
        if (
            settings.r2_modo != "off"
            and settings.r2_fonte_historico == "pipefy"
        ):
            total_d1_injetado = 0
            data_d1_iso = data_d1.isoformat()
            # Items de cards (coletas) do D-1
            for coleta in coletas:
                oc = coleta.oc
                placa_norm = oc.placa_normalizada
                if not placa_norm or not coleta.produtos_cotacao:
                    continue
                forn_id = str(oc.fornecedor.for_id) if oc.fornecedor else None
                forn_nome = oc.fornecedor.for_nome if oc.fornecedor else None
                card_id = coleta.card_pipefy.id if coleta.card_pipefy else None
                for p in coleta.produtos_cotacao:
                    chave_p = chave_produto(
                        ean=p.ean, codigo=p.cod_interno, descricao=p.descricao,
                    )
                    desc_raw = (p.descricao or "").strip()
                    historico_items_por_placa.setdefault(placa_norm, []).append({
                        "id_pedido": oc.id_pedido,
                        "id_cotacao": oc.id_cotacao,
                        "data_oc": data_d1_iso,
                        "identificador": oc.identificador,
                        "placa_normalizada": placa_norm,
                        "chave_produto": chave_p,
                        "descricao": desc_raw,
                        "descricao_normalizada": desc_raw.lower(),
                        "fornecedor_id": forn_id,
                        "fornecedor_nome": forn_nome,
                        "quantidade": float(p.quantidade or 0),
                        "card_pipefy_id": card_id,
                    })
                    total_d1_injetado += 1
            # Items de OCs orfas do D-1
            for (oc, _comprador), prods in zip(orfas_raw, produtos_por_orfa):
                placa_norm = oc.placa_normalizada
                if not placa_norm or not prods:
                    continue
                forn_id = str(oc.fornecedor.for_id) if oc.fornecedor else None
                forn_nome = oc.fornecedor.for_nome if oc.fornecedor else None
                for p in prods:
                    chave_p = chave_produto(
                        ean=p.ean, codigo=p.cod_interno, descricao=p.descricao,
                    )
                    desc_raw = (p.descricao or "").strip()
                    historico_items_por_placa.setdefault(placa_norm, []).append({
                        "id_pedido": oc.id_pedido,
                        "id_cotacao": oc.id_cotacao,
                        "data_oc": data_d1_iso,
                        "identificador": oc.identificador,
                        "placa_normalizada": placa_norm,
                        "chave_produto": chave_p,
                        "descricao": desc_raw,
                        "descricao_normalizada": desc_raw.lower(),
                        "fornecedor_id": forn_id,
                        "fornecedor_nome": forn_nome,
                        "quantidade": float(p.quantidade or 0),
                        "card_pipefy_id": None,
                    })
                    total_d1_injetado += 1
            if total_d1_injetado:
                logger.info(
                    "D-1 intra-batch: %d items do dia atual injetados no "
                    "historico para deteccao entre OCs do mesmo dia",
                    total_d1_injetado,
                )

        def _historico_indexado_para_oc(
            placa: str, id_pedido_atual: str
        ) -> dict[str, list[dict[str, Any]]] | None:
            """Monta o dict chave_produto -> [registros] para uma OC,
            excluindo os items que pertencem a ela mesma. Retorna None
            se o modo nao for pipefy (para que o fallback SQLite seja
            usado por detectar_reincidencias)."""
            if settings.r2_fonte_historico != "pipefy":
                return None
            items = historico_items_por_placa.get(placa, [])
            if not items:
                return {}
            id_str = str(id_pedido_atual or "").strip()
            filtrados = [
                it for it in items
                if str(it.get("id_pedido") or "").strip() != id_str
            ]
            return _indexar_historico_por_chave(filtrados)

        # 4e. Construir OcOrfas APLICANDO a R2 cross-time (agora que o
        # cache de devoluções e o histórico estão prontos). Para cards
        # com OC no Club, a R2 cross-time roda automaticamente dentro do
        # `aplicar_regras` mais abaixo. Para órfãs, precisamos rodar
        # manualmente porque elas não passam por `aplicar_regras`.
        ocs_orfas: list[OcOrfa] = []
        for (oc, comprador), produtos, qtd_cot_orfa in zip(orfas_raw, produtos_por_orfa, cotacoes_por_orfa):
            peca_dup_interna = _verificar_duplicidade_interna(produtos)
            forn_id = oc.fornecedor.for_id if oc.fornecedor else None
            hist_indexado_orfa = _historico_indexado_para_oc(
                oc.placa_normalizada, oc.id_pedido
            )
            divs_cross = detectar_reincidencias(
                placa_normalizada=oc.placa_normalizada,
                identificador=oc.identificador,
                id_pedido_atual=oc.id_pedido,
                fornecedor_id=str(forn_id) if forn_id else None,
                produtos=produtos,
                data_d1=data_d1,
                _historico_pipefy_items=hist_indexado_orfa,
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
                    qtd_cotacoes=qtd_cot_orfa if qtd_cot_orfa else None,
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
            hist_indexado_card = _historico_indexado_para_oc(
                coleta.oc.placa_normalizada, coleta.oc.id_pedido
            )
            contexto = ContextoValidacao(
                oc=coleta.oc,
                concorrentes=coleta.concorrentes,
                produtos_cotacao=coleta.produtos_cotacao,
                orcamento_cilia=coleta.orcamento_cilia,
                card_pipefy=coleta.card_pipefy,
                data_d1=data_d1,
                historico_indexado=hist_indexado_card,
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
                    card_campos=card.campos if card else {},
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
            # Link para o card no Pipefy
            card_link = None
            if r.card_pipefy_id:
                card_link = f"https://app.pipefy.com/pipes/{settings.pipe_id}#cards/{r.card_pipefy_id}"

            # Serializar divergências COMPLETAS (com dados/links)
            divergencias_completas = [
                {
                    "regra": d.regra,
                    "titulo": d.titulo,
                    "descricao": d.descricao,
                    "severidade": d.severidade.value if hasattr(d.severidade, 'value') else str(d.severidade),
                    "dados": d.dados,
                }
                for d in r.divergencias
            ]

            # Contagem de OCs por peça: quantas OCs distintas compraram
            # cada peça desta placa (incluindo a OC atual no batch).
            hist_placa = historico_items_por_placa.get(
                r.oc.placa_normalizada, []
            )
            # Agrupar por chave_produto → set de id_pedido distintos
            ocs_por_chave: dict[str, set[str]] = {}
            for it in hist_placa:
                ch = it.get("chave_produto") or ""
                ip = str(it.get("id_pedido") or "").strip()
                if ch and ip:
                    ocs_por_chave.setdefault(ch, set()).add(ip)

            # Serializar produtos da OC
            produtos_serializados = []
            for p in (r.produtos or []):
                ch_p = chave_produto(
                    ean=p.ean, codigo=p.cod_interno, descricao=p.descricao,
                )
                qtd_ocs = len(ocs_por_chave.get(ch_p, set()))
                produtos_serializados.append({
                    "descricao": getattr(p, "descricao", None),
                    "quantidade": getattr(p, "quantidade", 0),
                    "ean": getattr(p, "ean", None),
                    "cod_interno": getattr(p, "cod_interno", None),
                    "produto_id": getattr(p, "produto_id", None),
                    "valor_unitario": float(p.valor_unitario) if p.valor_unitario else None,
                    "valor_total": float(p.valor_total) if p.valor_total else None,
                    "qtd_ocs_com_peca": qtd_ocs,
                })

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
                    # --- Campos enriquecidos (Sessão 11) ---
                    "divergencias_json": divergencias_completas,
                    "produtos_json": produtos_serializados,
                    "reincidencia": r.reincidencia,
                    "cancelamento": r.cancelamento,
                    "cancelamento_card_id": r.cancelamento_card_id,
                    "card_pipefy_link": card_link,
                    "forma_pagamento_canonica": r.forma_pagamento_canonica,
                },
            )

        # 5b. Persistir OCs órfãs (Club sem card Pipefy) para o Dashboard
        for o in ocs_orfas:
            placa_orfa_norm = (
                o.identificador.replace("-", "").replace(" ", "").upper().strip()
                if o.identificador else ""
            )
            hist_placa_orfa = historico_items_por_placa.get(placa_orfa_norm, [])
            ocs_por_chave_orfa: dict[str, set[str]] = {}
            for it in hist_placa_orfa:
                ch = it.get("chave_produto") or ""
                ip = str(it.get("id_pedido") or "").strip()
                if ch and ip:
                    ocs_por_chave_orfa.setdefault(ch, set()).add(ip)

            produtos_orfa_ser = []
            for p in (o.produtos or []):
                ch_p = chave_produto(
                    ean=p.ean, codigo=p.cod_interno, descricao=p.descricao,
                )
                qtd_ocs = len(ocs_por_chave_orfa.get(ch_p, set()))
                produtos_orfa_ser.append({
                    "descricao": getattr(p, "descricao", None),
                    "quantidade": getattr(p, "quantidade", 0),
                    "ean": getattr(p, "ean", None),
                    "cod_interno": getattr(p, "cod_interno", None),
                    "produto_id": getattr(p, "produto_id", None),
                    "valor_unitario": float(p.valor_unitario) if p.valor_unitario else None,
                    "valor_total": float(p.valor_total) if p.valor_total else None,
                    "qtd_ocs_com_peca": qtd_ocs,
                })
            divs_orfa_ser = [
                {
                    "regra": d.regra,
                    "titulo": d.titulo,
                    "descricao": d.descricao,
                    "severidade": d.severidade.value if hasattr(d.severidade, 'value') else str(d.severidade),
                    "dados": d.dados,
                }
                for d in (o.divergencias or [])
            ]
            registrar_oc_resultado(
                validacao_id,
                {
                    "id_pedido": o.id_pedido,
                    "id_cotacao": o.id_cotacao,
                    "placa": o.identificador,
                    "placa_normalizada": o.identificador.replace("-", "").replace(" ", "").upper().strip() if o.identificador else None,
                    "fornecedor": o.fornecedor,
                    "comprador": o.comprador,
                    "forma_pagamento": o.forma_pagamento,
                    "valor_card": None,
                    "valor_club": float(o.valor) if o.valor else None,
                    "valor_pdf": None,
                    "valor_cilia": None,
                    "qtd_cotacoes": o.qtd_cotacoes,
                    "qtd_produtos": o.qtd_produtos,
                    "peca_duplicada": o.peca_duplicada,
                    "status": "sem_card_pipefy",
                    "regras_falhadas": [],
                    "fase_pipefy": None,
                    "card_pipefy_id": None,
                    "fase_pipefy_atual": None,
                    "divergencias_json": divs_orfa_ser,
                    "produtos_json": produtos_orfa_ser,
                    "reincidencia": o.reincidencia,
                    "cancelamento": o.cancelamento,
                    "cancelamento_card_id": o.cancelamento_card_id,
                    "card_pipefy_link": None,
                    "forma_pagamento_canonica": o.forma_pagamento,
                },
            )
        logger.info("Persistidas %d OCs órfãs em oc_resultados", len(ocs_orfas))

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
        return validacao_id, resultados, ocs_orfas, historico_status
