"""Testes do extrator de valor do PDF.

Gera um PDF sintético via reportlab apenas em runtime para não precisar
commitar binário. Se reportlab não estiver instalado, os testes são puláveis.
"""
from decimal import Decimal

import pytest

from app.clients.pdf_parser import _parse_valor_br, extrair_valor_total


def test_parse_valor_br_formatos():
    assert _parse_valor_br("R$ 1.234,56") == Decimal("1234.56")
    assert _parse_valor_br("1234.56") == Decimal("1234.56")
    assert _parse_valor_br("1.234,56") == Decimal("1234.56")
    assert _parse_valor_br("500,00") == Decimal("500.00")
    assert _parse_valor_br("") is None


def _gerar_pdf_total(total_str: str) -> bytes:
    """Gera um PDF em memória com um texto 'Total: <valor>'."""
    reportlab = pytest.importorskip("reportlab")
    from io import BytesIO

    from reportlab.pdfgen import canvas

    buf = BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(100, 750, "ORDEM DE COMPRA")
    c.drawString(100, 700, "Fornecedor: Exemplo Ltda")
    c.drawString(100, 680, "Placa: PAN1D24")
    c.drawString(100, 600, f"Valor Total: R$ {total_str}")
    c.save()
    return buf.getvalue()


def test_extrai_valor_com_marcador():
    pytest.importorskip("reportlab")
    pdf = _gerar_pdf_total("1.500,00")
    valor = extrair_valor_total(pdf)
    assert valor == Decimal("1500.00")


def test_retorna_none_se_sem_texto():
    # PDF vazio / corrompido
    valor = extrair_valor_total(b"not a pdf")
    assert valor is None
