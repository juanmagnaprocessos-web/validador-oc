"""Fixtures compartilhadas pytest."""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal

# Env mínimo para pydantic-settings não explodir ao importar app.config
os.environ.setdefault("CLUB_LOGIN", "teste@magna.local")
os.environ.setdefault("CLUB_SENHA", "teste123")
os.environ.setdefault("PIPEFY_TOKEN", "tok_de_teste")
os.environ.setdefault("PIPE_ID", "305587531")
os.environ.setdefault("CILIA_MODE", "stub")

import pytest  # noqa: E402

from app.models import (  # noqa: E402
    CardPipefy,
    Concorrente,
    ContextoValidacao,
    Fornecedor,
    OrdemCompra,
    ProdutoCotacao,
)


@pytest.fixture
def fornecedor_ativo() -> Fornecedor:
    return Fornecedor(
        for_id="42",
        for_nome="AUTOPEÇAS EXEMPLO LTDA",
        for_status="1",
        for_excluido="0",
    )


@pytest.fixture
def oc_valida(fornecedor_ativo: Fornecedor) -> OrdemCompra:
    return OrdemCompra(
        id_pedido="2032675",
        id_cotacao="1435474",
        identificador="PAN-1D24",
        valor_pedido=Decimal("1500.00"),
        forma="Pix",
        created_by=43773,
        comprador_nome="Marcelo Silva",
        comprador_email="marcelo@magnaprotecao.com.br",
        fornecedor=fornecedor_ativo,
        data_pedido=date(2026, 4, 5),
    )


@pytest.fixture
def contexto_ok(oc_valida: OrdemCompra) -> ContextoValidacao:
    return ContextoValidacao(
        oc=oc_valida,
        concorrentes=[
            Concorrente(id_fornecedor="1", fornecedor_nome="A"),
            Concorrente(id_fornecedor="2", fornecedor_nome="B"),
            Concorrente(id_fornecedor="3", fornecedor_nome="C"),
        ],
        produtos_cotacao=[
            ProdutoCotacao(produto_id="p1", descricao="Farol", quantidade=1, ean="111"),
            ProdutoCotacao(produto_id="p2", descricao="Capô", quantidade=1, ean="222"),
        ],
        orcamento_cilia=None,
        card_pipefy=CardPipefy(
            id="card-abc",
            title="PAN1D24",
            codigo_oc="2032675",
            # Cards completos no teste têm anexo + valor extraído OK por default,
            # para isolar a regra sob teste. Testes de edge-case sobrescrevem.
            anexo_oc_url="https://fake.pipefy.com/anexos/pan1d24.pdf",
            valor_extraido_pdf=Decimal("1500.00"),
        ),
        data_d1=date(2026, 4, 5),
    )
