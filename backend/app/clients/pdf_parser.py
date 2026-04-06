"""Extração de valor total de PDFs de Ordem de Compra (Regra R3).

O PDF é o anexo "Ordem de compra" dos cards do Pipefy. Estratégia:
1. Ler todo o texto com pdfplumber
2. Buscar marcadores ("Total", "Valor Total", "Total Geral", "Valor Final")
3. Na mesma linha ou linha seguinte, extrair primeiro valor monetário
4. Fallback: maior valor encontrado no documento
"""
from __future__ import annotations

import io
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pdfplumber

from app.logging_setup import get_logger

logger = get_logger(__name__)


# Marcadores ordenados por especificidade (mais específicos primeiro)
MARCADORES = [
    r"total\s+geral",
    r"valor\s+total\s+d[aoe]\s*pedido",
    r"valor\s+total\s+d[aoe]\s*ordem",
    r"valor\s+total",
    r"total\s+final",
    r"total\s+d[aoe]\s*pedido",
    r"total\s+d[aoe]\s*compra",
    r"valor\s+do\s+pedido",
    r"total\s*:",
    r"\btotal\b",
]

# Matches: R$ 1.234,56 / 1.234,56 / 1234.56 / R$1234,56
VALOR_RE = re.compile(
    r"R?\$?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+(?:[.,]\d{2}))",
    re.IGNORECASE,
)


def _parse_valor_br(s: str) -> Decimal | None:
    """Converte '1.234,56' ou '1234.56' em Decimal."""
    s = s.replace("R$", "").replace(" ", "").strip()
    if not s:
        return None

    # Formato BR: ponto de milhar + vírgula decimal
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        d = Decimal(s)
        return d.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _extrair_texto(source: bytes | str | Path) -> str:
    if isinstance(source, bytes):
        fh = io.BytesIO(source)
    else:
        fh = open(source, "rb")
    try:
        with pdfplumber.open(fh) as pdf:
            partes = []
            for page in pdf.pages:
                t = page.extract_text() or ""
                partes.append(t)
            return "\n".join(partes)
    finally:
        if hasattr(fh, "close"):
            fh.close()


def extrair_valor_total(source: bytes | str | Path) -> Decimal | None:
    """Extrai o valor total de uma OC em PDF.

    Args:
        source: bytes do PDF, caminho como str, ou Path

    Returns:
        Decimal com 2 casas, ou None se não encontrado.
    """
    try:
        texto = _extrair_texto(source)
    except Exception as e:
        logger.warning("Falha ao ler PDF: %s", e)
        return None

    if not texto.strip():
        logger.warning("PDF sem texto extraível — possivelmente escaneado")
        return None

    texto_lower = texto.lower()
    linhas = texto.split("\n")
    linhas_lower = [l.lower() for l in linhas]

    # 1) Buscar por marcadores conhecidos
    for padrao in MARCADORES:
        for idx, linha in enumerate(linhas_lower):
            if re.search(padrao, linha):
                # Procurar valor na mesma linha
                m = VALOR_RE.search(linhas[idx])
                if m:
                    v = _parse_valor_br(m.group(1))
                    if v is not None and v > 0:
                        logger.debug("Valor extraído via marcador '%s': %s", padrao, v)
                        return v
                # Tentar linha seguinte
                if idx + 1 < len(linhas):
                    m = VALOR_RE.search(linhas[idx + 1])
                    if m:
                        v = _parse_valor_br(m.group(1))
                        if v is not None and v > 0:
                            return v

    # 2) Fallback: maior valor do documento (assume que é o total)
    valores = []
    for m in VALOR_RE.finditer(texto):
        v = _parse_valor_br(m.group(1))
        if v is not None and v > 0:
            valores.append(v)

    if valores:
        maior = max(valores)
        logger.debug("Valor extraído via fallback (maior valor): %s", maior)
        return maior

    logger.warning("Nenhum valor encontrado no PDF")
    return None
