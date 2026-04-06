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
from app.db import init_db, registrar_oc_resultado, registrar_validacao
from app.logging_setup import get_logger
from app.models import (
    CardPipefy,
    Concorrente,
    ContextoValidacao,
    FasePipefy,
    Fornecedor,
    ItemOC,
    OrdemCompra,
    ProdutoCotacao,
    ResultadoValidacao,
    Severidade,
    StatusValidacao,
)
from app.services import compradores as compradores_svc
from app.validators import REGRAS_PADRAO, aplicar_regras

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
    s = str(v)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[: len(fmt) + 2], fmt).date()
        except ValueError:
            continue
    # Última tentativa: ISO
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
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


def _decidir_fase(
    oc: OrdemCompra, divergencias: list
) -> FasePipefy | None:
    # Mercado Livre: validação manual do analista — não movemos o card.
    # O estado AGUARDANDO_ML cuida da visibilidade no dashboard/relatório.
    if oc.eh_mercado_livre:
        return None

    # Divergência ERRO ou ALERTA → Informações Incorretas (INFO não conta)
    if any(d.severidade in (Severidade.ERRO, Severidade.ALERTA) for d in divergencias):
        return FasePipefy.INFORMACOES_INCORRETAS

    forma = (oc.forma or "").strip().lower()
    if "pix" in forma:
        return FasePipefy.PROGRAMAR_PAGAMENTO
    if "faturado" in forma or "cart" in forma or "vista" in forma:
        return FasePipefy.AGUARDAR_PECAS

    # Default conservador
    return FasePipefy.AGUARDAR_PECAS


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


async def _coletar_uma_oc(
    raw_oc: dict[str, Any],
    club: ClubClient,
    cilia: CiliaClient,
    pipefy: PipefyClient,
    cards_por_placa: dict[str, CardPipefy],
) -> ColetaOC:
    oc_basico = _parse_oc(raw_oc)

    tarefas = {}

    if oc_basico.id_cotacao:
        tarefas["concorrentes"] = club.get_concorrentes(oc_basico.id_cotacao)
        tarefas["produtos"] = club.get_produtos_cotacao(oc_basico.id_cotacao)

    tarefas["detalhes"] = club.get_order_details(oc_basico.id_pedido)

    if oc_basico.placa_normalizada:
        tarefas["cilia"] = cilia.consultar_por_placa(oc_basico.placa_normalizada)

    resultados = await asyncio.gather(
        *tarefas.values(), return_exceptions=True
    )
    res_map = dict(zip(tarefas.keys(), resultados))

    def _ok(key: str):
        v = res_map.get(key)
        if isinstance(v, Exception):
            logger.warning("Falha em %s para OC %s: %s", key, oc_basico.id_pedido, v)
            return None
        return v

    concorrentes_raw = _ok("concorrentes") or []
    produtos_raw = _ok("produtos") or []
    detalhes = _ok("detalhes") or {}
    orcamento = _ok("cilia")

    # Enriquecer OC com dados de detalhes
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
    produtos = [
        ProdutoCotacao(
            produto_id=str(p.get("produto_id") or p.get("id") or ""),
            descricao=p.get("descricao") or p.get("name"),
            quantidade=float(p.get("quantidade") or p.get("quantity") or 0),
            ean=p.get("ean"),
            cod_interno=p.get("cod_interno"),
        )
        for p in produtos_raw
    ]

    # Localiza card no Pipefy por placa (título = placa sem hífen)
    card = cards_por_placa.get(oc_basico.placa_normalizada)
    if card and card.anexo_oc_url and card.valor_extraido_pdf is None:
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
    pipefy: PipefyClient, resultado: ResultadoValidacao
) -> None:
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

    # 1) Preencher campos de validação
    await pipefy.update_card_field(
        resultado.card_pipefy_id, "peca_duplicada", resultado.peca_duplicada
    )
    await pipefy.update_card_field(
        resultado.card_pipefy_id, "abatimento_fornecedor", resultado.abatimento_fornecedor
    )
    await pipefy.update_card_field(
        resultado.card_pipefy_id,
        "validacao_concluida_por",
        settings.validador_identificador,
    )
    await pipefy.update_card_field(
        resultado.card_pipefy_id,
        "validacao_concluida",
        "Sim" if resultado.aprovada else "Não",
    )

    # 1b) Se houver divergência, registrar justificativa no campo
    # "Informe a negativa da validação" (descoberto via introspecção).
    if resultado.divergencias:
        texto = "\n".join(
            f"[{d.regra}] {d.titulo}: {d.descricao}"
            for d in resultado.divergencias
        )
        try:
            await pipefy.update_card_field(
                resultado.card_pipefy_id, "justificativa_divergencia", texto
            )
        except Exception as e:
            logger.debug("Campo justificativa_divergencia não atualizado: %s", e)

    # 2) Mover de fase
    if resultado.fase_destino:
        chave = FASE_ENUM_PARA_CHAVE.get(resultado.fase_destino)
        if chave:
            await pipefy.mover_card(resultado.card_pipefy_id, chave)


# ======================================================================
# Orquestrador principal
# ======================================================================

async def executar_validacao(
    data_d1: date,
    *,
    dry_run: bool = True,
    concorrencia: int = 5,
) -> tuple[int, list[ResultadoValidacao]]:
    """Executa o pipeline completo. Retorna (validacao_id, resultados)."""
    init_db()
    logger.info(
        "=== Início validação D-1=%s dry_run=%s ===", data_d1, dry_run
    )

    cilia = build_cilia_client()

    async with ClubClient() as club, PipefyClient(dry_run=dry_run) as pipefy:
        # 1. Coletar OCs do D-1
        pedidos_raw = await club.listar_pedidos(data_d1)
        logger.info("Club: %d OCs encontradas em %s", len(pedidos_raw), data_d1)

        # 2. Listar cards de TODAS as fases relevantes e indexar por placa.
        # Precisamos incluir as fases destino (Programar Pagamento, Aguardar
        # Peças, etc.) para detectar cards JÁ PROCESSADOS que foram movidos
        # manualmente pelo analista — senão o matching por placa falha e
        # o validador reprocessa como "sem card", gerando divergências falsas.
        fases_para_scan = [
            "validacao",
            "aguardar_pecas",
            "programar_pagamento",
            "compras_ml",
            "informacoes_incorretas",
        ]
        cards_por_placa: dict[str, CardPipefy] = {}
        for fase_chave in fases_para_scan:
            try:
                cards_fase = await pipefy.listar_cards_fase(fase_chave)
            except Exception as e:
                logger.warning("Falha ao listar fase %s: %s", fase_chave, e)
                continue
            for c in cards_fase:
                placa = (c.title or "").replace("-", "").upper().strip()
                if not placa:
                    continue
                # Prioriza card na fase "validacao" se houver colisão
                if placa in cards_por_placa and fase_chave != "validacao":
                    continue
                cards_por_placa[placa] = c
        logger.info("Pipefy: %d cards indexados em %d fases", len(cards_por_placa), len(fases_para_scan))

        # 3. Coleta paralela por OC (com semáforo)
        semaforo = asyncio.Semaphore(concorrencia)

        async def _com_sem(raw):
            async with semaforo:
                return await _coletar_uma_oc(
                    raw, club, cilia, pipefy, cards_por_placa
                )

        coletas = await asyncio.gather(*[_com_sem(p) for p in pedidos_raw])

        # 3b. Resolver nome/email dos compradores (created_by -> tabela auxiliar)
        compradores_svc.init_table()
        for coleta in coletas:
            cb = coleta.oc.created_by
            if cb:
                nome, email = compradores_svc.resolve(cb)
                coleta.oc.comprador_nome = nome
                coleta.oc.comprador_email = email

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

            # Hierarquia de status (ordem importa):
            #  1. ML — sempre, mesmo sem divergência
            #  2. Card já fora da fase "Validação" — não reprocessa
            #  3. Divergência bloqueante
            #  4. Aprovada
            if coleta.oc.eh_mercado_livre:
                status = StatusValidacao.AGUARDANDO_ML
                fase = None
            elif (
                card
                and card.phase_id
                and fase_validacao_id
                and card.phase_id != fase_validacao_id
            ):
                status = StatusValidacao.JA_PROCESSADA
                fase = None
            elif bloqueantes:
                status = StatusValidacao.DIVERGENCIA
                fase = _decidir_fase(coleta.oc, divergencias)
            else:
                status = StatusValidacao.APROVADA
                fase = _decidir_fase(coleta.oc, divergencias)

            # Peça duplicada: checa se R2 acusou duplicidade
            peca_dup = "Não"
            for d in divergencias:
                if d.regra == "R2" and "duplicada" in d.titulo.lower():
                    peca_dup = "Sim"
                    break

            resultados.append(
                ResultadoValidacao(
                    oc=coleta.oc,
                    status=status,
                    divergencias=divergencias,
                    fase_destino=fase,
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

        # 6. Atuar no Pipefy
        if not dry_run:
            logger.info("Aplicando %d resultados no Pipefy...", len(resultados))
            for r in resultados:
                try:
                    await _atuar_no_pipefy(pipefy, r)
                except Exception as e:
                    logger.error("Falha ao atuar no Pipefy para %s: %s", r.oc.id_pedido, e)
        else:
            logger.info("Dry run — Pipefy não foi alterado.")

        logger.info(
            "=== Fim validação: %d OCs — aprovadas=%d divergentes=%d "
            "bloqueadas=%d ml=%d ja_processadas=%d ===",
            len(resultados), aprovadas, divergentes, bloqueadas,
            aguardando_ml, ja_processadas,
        )

        await cilia.close()
        return validacao_id, resultados
