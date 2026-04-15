"""Lógica do job diário do CRON: lock persistente, probe leve (Club+Pipefy),
early-exit se vazio, retry com backoff, registro final em `cron_locks`.

Invocado pelo APScheduler (ver `scheduler.py`) todos os dias às
`CRON_HOUR_BRT:CRON_MINUTE`. Calcula D-1 em horário BRT, tenta aquisição
de lock e, se conseguiu, executa o pipeline completo do orchestrator em
modo dry-run (quando `settings.cron_dry_run=True`).

Early-exit: se Club e Pipefy retornarem zero pedidos/cards para o D-1,
não gera relatório nem linha em `validacoes` — registra apenas status
'vazio' no lock. Se apenas uma das fontes for zero, roda normal (é
exatamente o sinal que R3 deve detectar).
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from app.config import settings
from app.db import (
    adquirir_cron_lock,
    finalizar_cron_lock,
    get_conn,
)

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _warmup_db() -> None:
    """Absorve cold start do Neon (branch suspenso após 5min ociosos).

    Primeira query de uma conexão fria ao Postgres no Neon free tier
    pode estourar timeout de ~5s. 3 retries × 2s de espera cobre a
    janela de boot.
    """
    with get_conn() as conn:
        conn.execute("SELECT 1")


def _host_id() -> str:
    try:
        return f"{socket.gethostname()}:{os.getpid()}"
    except Exception:
        return f"unknown:{os.getpid()}"


def _computar_data_d1(agora: datetime | None = None) -> date:
    """D-1 em horário BRT. Se `agora` não fornecido, usa now(BRT)."""
    tz = ZoneInfo(settings.cron_timezone)
    now = agora or datetime.now(tz)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    return (now.astimezone(tz).date() - timedelta(days=1))


async def _probe_vazio(data_d1: date) -> bool:
    """True quando Club=0 AND Pipefy=0 para o D-1 (então early-exit).

    Se alguma das fontes tiver >0, retorna False — mesmo que a outra seja
    zero. Esse cenário (Club>0, Pipefy=0) é EXATAMENTE o sinal que a R3
    deve pegar, então early-exit aqui esconderia bug.
    """
    from app.clients.club_client import ClubClient
    from app.clients.pipefy_client import PipefyClient

    try:
        async with ClubClient() as cc:
            pedidos = await cc.listar_pedidos(data_d1)
        if len(pedidos) > 0:
            return False
    except Exception as e:
        logger.warning("Probe Club falhou — assumindo não-vazio: %s", e)
        return False

    try:
        async with PipefyClient() as pc:
            cards = await pc.listar_cards_fase("validacao", max_cards=500)
        cards_d1 = [c for c in cards if c.created_at and c.created_at.date() == data_d1]
        if len(cards_d1) > 0:
            return False
    except Exception as e:
        logger.warning("Probe Pipefy falhou — assumindo não-vazio: %s", e)
        return False

    logger.info("Probe detectou D-1 vazio (Club=0, Pipefy=0) — early-exit")
    return True


async def _executar_uma_tentativa(data_d1: date) -> str:
    """Executa pipeline + gera relatórios. Retorna 'sucesso' ou 'vazio'.

    Lança exceção em caso de falha — caller trata retry.

    Serializa com o endpoint manual POST /api/validar via validation_lock,
    evitando colisão (backup_db, escrita de relatório, INSERT validacoes)
    se o analista disparar validação manual enquanto o CRON roda.
    """
    from app.services.orchestrator import executar_validacao
    from app.services.report import gerar_excel, gerar_html
    from app.services.validation_lock import get_lock

    if await _probe_vazio(data_d1):
        return "vazio"

    async with get_lock():
        validacao_id, resultados, ocs_orfas, historico_status = await executar_validacao(
            data_d1,
            dry_run=settings.cron_dry_run,
            origem="cron",
        )
        gerar_html(
            data_d1, resultados, dry_run=settings.cron_dry_run,
            ocs_orfas=ocs_orfas, historico_status=historico_status,
        )
        gerar_excel(
            data_d1, resultados, ocs_orfas=ocs_orfas,
            historico_status=historico_status,
        )
    logger.info(
        "CRON sucesso — validacao_id=%d D-1=%s OCs=%d",
        validacao_id, data_d1, len(resultados),
    )
    return "sucesso"


async def run_daily_validation_job(data_d1_override: date | None = None) -> None:
    """Ponto de entrada do APScheduler. Não levanta exceções — toda falha
    é registrada em `cron_locks`.

    Parâmetro `data_d1_override` permite uso via endpoint admin
    (`POST /api/admin/cron/run-now?data_d1=YYYY-MM-DD`).
    """
    data_d1 = data_d1_override or _computar_data_d1()
    data_d1_iso = data_d1.isoformat()
    host = _host_id()

    try:
        _warmup_db()
    except Exception as e:
        logger.error("Warm-up DB falhou 3x — abortando CRON: %s", e)
        return

    if not adquirir_cron_lock(
        data_d1_iso, host, settings.cron_lock_ttl_s, tentativa=1
    ):
        logger.info(
            "CRON skip — lock já em posse ou dia já processado (D-1=%s)",
            data_d1_iso,
        )
        return

    retry_delays = settings.cron_retry_delays_list  # ex: [15, 45]
    max_tentativas = 1 + len(retry_delays)
    last_err: str | None = None

    for tentativa in range(1, max_tentativas + 1):
        if tentativa > 1:
            # Re-adquirir lock para atualizar `tentativa` e TTL
            adquirir_cron_lock(
                data_d1_iso, host, settings.cron_lock_ttl_s, tentativa=tentativa
            )
        try:
            resultado = await _executar_uma_tentativa(data_d1)
            finalizar_cron_lock(data_d1_iso, resultado, last_error=None)
            return
        except Exception as e:  # noqa: BLE001 — queremos capturar tudo
            last_err = f"{type(e).__name__}: {e}"
            logger.exception(
                "CRON tentativa %d/%d falhou: %s",
                tentativa, max_tentativas, last_err,
            )
            if tentativa < max_tentativas:
                delay_min = retry_delays[tentativa - 1]
                finalizar_cron_lock(data_d1_iso, "rodando", last_error=last_err)
                logger.info("CRON retry em %d minutos", delay_min)
                await asyncio.sleep(delay_min * 60)

    finalizar_cron_lock(data_d1_iso, "falha", last_error=last_err)
    logger.error("CRON esgotou %d tentativas para D-1=%s", max_tentativas, data_d1_iso)
