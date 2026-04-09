"""Função canônica para gerar chave única de produto.

Usada por r2_duplicidade, historico_produtos e orchestrator para
garantir que a MESMA lógica de chave é aplicada em todos os contextos.

Prioridade: EAN > código interno > descrição.
"""
from __future__ import annotations

from typing import Any


def chave_produto(
    *,
    ean: str | None = None,
    codigo: str | None = None,
    descricao: str | None = None,
) -> str:
    """Gera chave única normalizada para um produto.

    Prioridade: EAN > código > descrição.
    Sempre aplica strip() para evitar divergências por espaços.
    """
    ean_clean = (ean or "").strip()
    if ean_clean:
        return f"ean:{ean_clean}"
    cod_clean = (codigo or "").strip()
    if cod_clean:
        return f"cod:{cod_clean}"
    desc_clean = (descricao or "").strip().lower()
    if desc_clean:
        return f"desc:{desc_clean}"
    return "sem_chave"


def chave_produto_de_obj(p: Any) -> str:
    """Gera chave a partir de um objeto com atributos (ex: ProdutoCotacao).

    Lê via getattr (compatível com dataclass, Pydantic, namedtuple).
    """
    return chave_produto(
        ean=str(getattr(p, "ean", None) or ""),
        codigo=str(getattr(p, "cod_interno", None) or ""),
        descricao=str(getattr(p, "descricao", None) or ""),
    )


def chave_produto_de_dict(p: dict[str, Any]) -> str:
    """Gera chave a partir de um dict (ex: item do Club API).

    Aceita campos ean, cod_interno, descricao (e fallback 'name').
    """
    ean_raw = p.get("ean")
    ean = str(ean_raw).strip() if ean_raw and isinstance(ean_raw, str) else ""

    cod_raw = p.get("cod_interno")
    cod = str(cod_raw).strip() if cod_raw and isinstance(cod_raw, str) else ""

    desc_raw = p.get("descricao") or p.get("name") or ""
    desc = str(desc_raw) if desc_raw else ""

    return chave_produto(ean=ean, codigo=cod, descricao=desc)
