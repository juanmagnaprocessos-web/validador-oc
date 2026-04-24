"""APScheduler wrapper para o CRON diário das 02:00 BRT.

O scheduler é inicializado no `lifespan` do FastAPI (`main.py`) quando
`settings.cron_enabled` for True. Timezone é EXPLÍCITO (ZoneInfo BRT) em
duas camadas (`AsyncIOScheduler.timezone` e `CronTrigger.timezone`) para
blindar contra servidores em UTC.

Expomos `get_scheduler()` como singleton de módulo para permitir que
endpoints admin disparem o job fora do horário (`POST /api/admin/cron/run-now`).
"""
from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def get_scheduler() -> AsyncIOScheduler | None:
    return _scheduler


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        logger.warning("Scheduler já iniciado — ignorando chamada duplicada")
        return
    if not settings.cron_enabled:
        logger.info("CRON desabilitado (CRON_ENABLED=false) — scheduler não inicia")
        return

    from app.services.cron_runner import run_daily_validation_job

    tz = ZoneInfo(settings.cron_timezone)
    _scheduler = AsyncIOScheduler(
        timezone=tz,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": settings.cron_misfire_grace_s,
        },
    )
    _scheduler.add_job(
        run_daily_validation_job,
        CronTrigger(
            hour=settings.cron_hour_brt,
            minute=settings.cron_minute,
            timezone=tz,
        ),
        id="daily-validation",
        replace_existing=True,
    )

    # Job diario de purge do log de tentativas (retention = LOGIN_ATTEMPTS_
    # RETENTION_DAYS). Roda as 03h BRT pra nao competir com a validacao
    # principal. Idempotente: se nao ha nada a apagar, retorna 0.
    def _purge_login_attempts_job() -> None:
        try:
            from app.services.login_attempts import purgar_logs_antigos
            res = purgar_logs_antigos()
            logger.info(
                "login_attempts purge: removidos=%d corte=%s retention=%dd",
                res["removidos"], res["corte_iso"], res["retention_days"],
            )
        except Exception:
            logger.exception("Falha ao rodar purge de login_attempts")

    _scheduler.add_job(
        _purge_login_attempts_job,
        CronTrigger(hour=3, minute=0, timezone=tz),
        id="login-attempts-purge",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info(
        "CRON iniciado — job 'daily-validation' em %02d:%02d %s; "
        "'login-attempts-purge' em 03:00 %s",
        settings.cron_hour_brt, settings.cron_minute, settings.cron_timezone,
        settings.cron_timezone,
    )


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    _scheduler.shutdown(wait=False)
    _scheduler = None
    logger.info("CRON parado")
