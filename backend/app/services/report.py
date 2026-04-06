"""Geração de relatórios consolidados HTML + Excel."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import logging

from jinja2 import Environment, FileSystemLoader, select_autoescape
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from app.config import BASE_DIR, settings
from app.models import ResultadoValidacao, StatusValidacao


_jinja = Environment(
    loader=FileSystemLoader(BASE_DIR / "templates"),
    autoescape=select_autoescape(["html"]),
)


def gerar_html(
    data_d1: date,
    resultados: list[ResultadoValidacao],
    *,
    dry_run: bool,
) -> Path:
    template = _jinja.get_template("relatorio.html.j2")
    total = len(resultados)
    aprovadas = sum(1 for r in resultados if r.status == StatusValidacao.APROVADA)
    divergentes = sum(1 for r in resultados if r.status == StatusValidacao.DIVERGENCIA)
    bloqueadas = sum(1 for r in resultados if r.status == StatusValidacao.BLOQUEADA)
    aguardando_ml = sum(1 for r in resultados if r.status == StatusValidacao.AGUARDANDO_ML)
    ja_processadas = sum(1 for r in resultados if r.status == StatusValidacao.JA_PROCESSADA)

    html = template.render(
        data_d1=data_d1.isoformat(),
        gerado_em=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        executado_por=settings.validador_identificador,
        dry_run=dry_run,
        total=total,
        aprovadas=aprovadas,
        divergentes=divergentes,
        bloqueadas=bloqueadas,
        aguardando_ml=aguardando_ml,
        ja_processadas=ja_processadas,
        resultados=resultados,
        pipe_id=settings.pipe_id,
    )

    out = settings.relatorios_full_dir / f"{data_d1.isoformat()}_validacao.html"
    out.write_text(html, encoding="utf-8")
    return out


def gerar_excel(data_d1: date, resultados: list[ResultadoValidacao]) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = f"Validação {data_d1.isoformat()}"

    headers = [
        "Nº OC",
        "Placa (com hífen)",
        "Placa (sem hífen)",
        "Fornecedor",
        "Comprador",
        "Forma Pagamento",
        "Valor OC (Club)",
        "Valor PDF (Pipefy)",
        "Valor Cilia",
        "Qtd Cotações",
        "Qtd Produtos",
        "Peça Duplicada",
        "Status",
        "Motivo",
        "Fase Pipefy Atual",
        "Fase Pipefy Destino",
    ]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1A2332")
    header_font = Font(bold=True, color="FFFFFF")
    for col_idx, _ in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col_idx)
        c.fill = header_fill
        c.font = header_font
        c.alignment = Alignment(horizontal="left", vertical="center")

    status_fill = {
        "aprovada": PatternFill("solid", fgColor="D1FAE5"),
        "divergencia": PatternFill("solid", fgColor="FED7AA"),
        "bloqueada": PatternFill("solid", fgColor="FECACA"),
        "aguardando_ml": PatternFill("solid", fgColor="FEF3C7"),
        "ja_processada": PatternFill("solid", fgColor="E5E7EB"),
    }
    status_label = {
        "aprovada": "Aprovada",
        "divergencia": "Divergência",
        "bloqueada": "Bloqueada",
        "aguardando_ml": "Aguardando ML",
        "ja_processada": "Já processada",
    }

    for row_idx, r in enumerate(resultados, start=2):
        forn = r.oc.fornecedor.for_nome if r.oc.fornecedor else ""
        motivo = r.motivo_resumido
        if r.status == StatusValidacao.AGUARDANDO_ML:
            motivo = "Fornecedor Mercado Livre — validação manual do analista"
        elif r.status == StatusValidacao.JA_PROCESSADA:
            motivo = f"Card já estava em fase '{r.fase_pipefy_atual or '?'}'"
        ws.append([
            r.oc.id_pedido,
            r.oc.identificador or "",
            r.oc.placa_normalizada,
            forn,
            r.oc.comprador_nome or (f"#{r.oc.created_by}" if r.oc.created_by else ""),
            r.oc.forma or "",
            float(r.valor_club) if r.valor_club else None,
            float(r.valor_pdf) if r.valor_pdf else None,
            float(r.valor_cilia) if r.valor_cilia else None,
            r.qtd_cotacoes,
            r.qtd_produtos,
            r.peca_duplicada,
            status_label.get(r.status.value, r.status.value),
            motivo,
            r.fase_pipefy_atual or "",
            r.fase_destino.value if r.fase_destino else "",
        ])
        fill = status_fill.get(r.status.value)
        if fill:
            ws.cell(row=row_idx, column=13).fill = fill

    # Formato de moeda
    for col in (7, 8, 9):
        for row in range(2, len(resultados) + 2):
            ws.cell(row=row, column=col).number_format = "R$ #,##0.00"

    # Larguras automáticas
    for col_idx, header in enumerate(headers, start=1):
        largura = max(
            len(header),
            max(
                (len(str(ws.cell(row=r, column=col_idx).value or ""))
                 for r in range(2, len(resultados) + 2)),
                default=0,
            ),
        )
        ws.column_dimensions[get_column_letter(col_idx)].width = min(largura + 2, 50)

    ws.freeze_panes = "A2"

    out = settings.relatorios_full_dir / f"{data_d1.isoformat()}_validacao.xlsx"
    try:
        wb.save(out)
    except PermissionError:
        # Arquivo aberto no Excel — salva com timestamp e avisa
        ts = datetime.now().strftime("%H%M%S")
        out = settings.relatorios_full_dir / f"{data_d1.isoformat()}_validacao_{ts}.xlsx"
        wb.save(out)
        logging.getLogger(__name__).warning(
            "Arquivo Excel estava aberto — salvo como %s", out.name
        )
    return out
