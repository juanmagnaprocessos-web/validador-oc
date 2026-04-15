"""Rotas de validacao."""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

from app.config import settings
from app.db import (
    dry_runs_cron_pendentes,
    listar_historico,
    resultados_de,
    ultima_falha_cron,
    ultimo_cron_lock,
)
from app.models import StatusValidacao, Usuario
from app.services.auth import get_current_user, require_admin
from app.services.orchestrator import executar_validacao
from app.services.report import gerar_excel, gerar_html
from app.services.validation_lock import get_lock as _get_validacao_lock

_DATA_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

router = APIRouter(tags=["validacao"])

# Lock global: apenas uma validacao por vez (compartilhado com CRON)
_validacao_lock = _get_validacao_lock()

# Referências de tasks CRON em background — evita GC prematuro dos tasks
# spawnados por /admin/cron/run-now (asyncio.create_task retorna ref que
# pode ser coletada se ninguém segurar).
_cron_tasks: set[asyncio.Task] = set()


@router.post("/validar")
async def validar(
    data: str = Query(..., description="D-1 no formato YYYY-MM-DD"),
    dry_run: bool = Query(True),
    _: Usuario = Depends(get_current_user),
):
    if _validacao_lock.locked():
        raise HTTPException(429, "Outra validacao ja esta em andamento")

    async with _validacao_lock:
        try:
            data_d1 = date.fromisoformat(data)
        except ValueError:
            raise HTTPException(422, "data deve estar em YYYY-MM-DD")

        validacao_id, resultados, ocs_orfas, historico_status = await executar_validacao(
            data_d1, dry_run=dry_run
        )
        html_path = gerar_html(
            data_d1, resultados, dry_run=dry_run,
            ocs_orfas=ocs_orfas, historico_status=historico_status,
        )
        xlsx_path = gerar_excel(
            data_d1, resultados, ocs_orfas=ocs_orfas,
            historico_status=historico_status,
        )

        def _count(status: StatusValidacao) -> int:
            return sum(1 for r in resultados if r.status == status)

        return {
            "validacao_id": validacao_id,
            "data_d1": data_d1.isoformat(),
            "total": len(resultados),
            "aprovadas": _count(StatusValidacao.APROVADA),
            "divergentes": _count(StatusValidacao.DIVERGENCIA),
            "bloqueadas": _count(StatusValidacao.BLOQUEADA),
            "aguardando_ml": _count(StatusValidacao.AGUARDANDO_ML),
            "ja_processadas": _count(StatusValidacao.JA_PROCESSADA),
            "ocs_orfas": len(ocs_orfas),
            "dry_run": dry_run,
            "relatorio_html": str(html_path.name),
            "relatorio_xlsx": str(xlsx_path.name),
            "cilia_mode": settings.cilia_mode,
            "cilia_base_url": settings.cilia_base_url,
        }


def _validar_data_iso(data: str) -> str:
    """Valida data YYYY-MM-DD e retorna canonicalizada (anti path traversal)."""
    try:
        d = date.fromisoformat(data)
    except ValueError:
        raise HTTPException(422, "data inválida — use YYYY-MM-DD")
    return d.isoformat()


@router.get("/relatorio/{data}")
async def ver_relatorio(data: str, _: Usuario = Depends(get_current_user)):
    data_iso = _validar_data_iso(data)
    html = Path(settings.relatorios_full_dir) / f"{data_iso}_validacao.html"
    if not html.exists():
        raise HTTPException(404, "Relatório não encontrado. Rode /validar antes.")
    return FileResponse(html, media_type="text/html")


@router.get("/relatorio/{data}/excel")
async def baixar_excel(data: str, _: Usuario = Depends(get_current_user)):
    data_iso = _validar_data_iso(data)
    xlsx = Path(settings.relatorios_full_dir) / f"{data_iso}_validacao.xlsx"
    if not xlsx.exists():
        raise HTTPException(404, "Excel não encontrado.")
    return FileResponse(
        xlsx,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=xlsx.name,
    )


@router.get("/historico")
async def historico(
    limite: int = Query(30, ge=1, le=500, description="Limite de registros (1 a 500)"),
    data_inicio: str | None = Query(None, description="D-1 mínima YYYY-MM-DD"),
    data_fim: str | None = Query(None, description="D-1 máxima YYYY-MM-DD"),
    _: Usuario = Depends(get_current_user),
):
    if data_inicio is not None and not _DATA_ISO_RE.match(data_inicio):
        raise HTTPException(422, "data_inicio inválida — use YYYY-MM-DD")
    if data_fim is not None and not _DATA_ISO_RE.match(data_fim):
        raise HTTPException(422, "data_fim inválida — use YYYY-MM-DD")
    if data_inicio and data_fim and data_inicio > data_fim:
        raise HTTPException(422, "data_inicio não pode ser maior que data_fim")
    return JSONResponse(
        listar_historico(limite=limite, data_inicio=data_inicio, data_fim=data_fim)
    )


@router.get("/cron/status")
async def cron_status(_: Usuario = Depends(get_current_user)):
    """Estado atual do CRON: última execução, última falha e dry-runs pendentes.

    Alimenta os banners do Dashboard (vermelho se última execução falhou;
    amarelo se houver validações CRON dry-run ainda não aplicadas).
    """
    return {
        "enabled": settings.cron_enabled,
        "hora_brt": f"{settings.cron_hour_brt:02d}:{settings.cron_minute:02d}",
        "dry_run": settings.cron_dry_run,
        "ultimo_lock": ultimo_cron_lock(),
        "ultima_falha": ultima_falha_cron(),
        "dry_runs_pendentes": dry_runs_cron_pendentes(dias=3),
    }


@router.post("/admin/cron/run-now")
async def cron_run_now(
    data_d1: str | None = Query(None, description="D-1 opcional YYYY-MM-DD (default: ontem)"),
    _: Usuario = Depends(require_admin),
):
    """Dispara o job CRON manualmente (fora do horário). Útil para smoke test
    e backfill pontual. Reusa toda a lógica (lock, probe, retry, relatório).

    Proteções contra abuso:
      - data_d1 limitada aos últimos 90 dias (evita backfill massivo)
      - rejeita se já existe lock 'rodando' (evita fire-and-forget spam)
    """
    from datetime import timedelta
    from app.services.cron_runner import run_daily_validation_job

    if data_d1 is not None:
        if not _DATA_ISO_RE.match(data_d1):
            raise HTTPException(422, "data_d1 inválida — use YYYY-MM-DD")
        data_override = date.fromisoformat(data_d1)
        if data_override > date.today() or data_override < date.today() - timedelta(days=90):
            raise HTTPException(422, "data_d1 deve estar entre hoje-90d e hoje")
    else:
        data_override = None

    ultimo = ultimo_cron_lock()
    if ultimo and ultimo.get("status") == "rodando":
        raise HTTPException(429, f"CRON já em execução para D-1={ultimo.get('data_d1')}")

    async def _dispatch():
        try:
            await run_daily_validation_job(data_d1_override=data_override)
        except Exception:
            logging.getLogger(__name__).exception("CRON run-now falhou")

    task = asyncio.create_task(_dispatch())
    _cron_tasks.add(task)
    task.add_done_callback(_cron_tasks.discard)
    return {"status": "disparado", "data_d1": data_d1 or "ontem-BRT"}


@router.get("/config")
async def config_publica(_: Usuario = Depends(get_current_user)):
    """Config expose ao frontend — flags que controlam exibicao de botoes
    e links (ex: botao 'Verificar no Cilia'). Valores vem de settings.
    """
    return {
        "cilia_mode": settings.cilia_mode,
        "cilia_base_url": settings.cilia_base_url,
        "r2_modo": settings.r2_modo,
        "modo_operacao": settings.modo_operacao,
    }


@router.get("/validacoes/{validacao_id}/resultados")
async def resultados_da_validacao(
    validacao_id: int, _: Usuario = Depends(get_current_user)
):
    return JSONResponse(resultados_de(validacao_id))
