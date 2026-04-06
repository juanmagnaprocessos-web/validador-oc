"""Rotas de validação."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

from app.config import settings
from app.db import listar_historico, resultados_de
from app.models import StatusValidacao
from app.services.orchestrator import executar_validacao
from app.services.report import gerar_excel, gerar_html

router = APIRouter(tags=["validação"])


@router.post("/validar")
async def validar(
    data: str = Query(..., description="D-1 no formato YYYY-MM-DD"),
    dry_run: bool = Query(True),
):
    try:
        data_d1 = date.fromisoformat(data)
    except ValueError:
        raise HTTPException(422, "data deve estar em YYYY-MM-DD")

    validacao_id, resultados = await executar_validacao(
        data_d1, dry_run=dry_run
    )
    html_path = gerar_html(data_d1, resultados, dry_run=dry_run)
    xlsx_path = gerar_excel(data_d1, resultados)

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
        "dry_run": dry_run,
        "relatorio_html": str(html_path.name),
        "relatorio_xlsx": str(xlsx_path.name),
    }


@router.get("/relatorio/{data}")
async def ver_relatorio(data: str):
    try:
        date.fromisoformat(data)
    except ValueError:
        raise HTTPException(422, "data inválida")

    html = Path(settings.relatorios_full_dir) / f"{data}_validacao.html"
    if not html.exists():
        raise HTTPException(404, "Relatório não encontrado. Rode /validar antes.")
    return FileResponse(html, media_type="text/html")


@router.get("/relatorio/{data}/excel")
async def baixar_excel(data: str):
    xlsx = Path(settings.relatorios_full_dir) / f"{data}_validacao.xlsx"
    if not xlsx.exists():
        raise HTTPException(404, "Excel não encontrado.")
    return FileResponse(
        xlsx,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=xlsx.name,
    )


@router.get("/historico")
async def historico(limite: int = 30):
    return JSONResponse(listar_historico(limite))


@router.get("/validacoes/{validacao_id}/resultados")
async def resultados_da_validacao(validacao_id: int):
    return JSONResponse(resultados_de(validacao_id))
