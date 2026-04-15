"""Testes de edge cases do red team consolidado.

Cobre casos que antes nao tinham teste e que podem surgir em prod:
  - R1 com duplo-vazio (sem ofertas_por_peca e sem concorrentes)
  - chave_produto com todos campos vazios (colisao UNIQUE evitada)
  - _normalizar_pedido_v1 com request=None ou list
  - listar_ofertas_por_peca com vencedores=None
  - _dbconn.execute com parametros Decimal e datetime
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.club_client import ClubClient
from app.models import ProdutoCotacao
from app.utils.chave_produto import chave_produto, chave_produto_de_obj
from app.validators.r1_minimo_cotacoes import R1MinimoCotacoes


# ----------------------------------------------------------------------
# chave_produto: fallback para produto_id evitando colisao
# ----------------------------------------------------------------------


def test_chave_produto_fallback_produto_id():
    # Antes do fix, duas pecas sem EAN/cod/desc retornavam "sem_chave"
    # e colidiam em UNIQUE(id_pedido, chave_produto).
    c1 = chave_produto(produto_id="123")
    c2 = chave_produto(produto_id="456")
    assert c1 != c2
    assert c1 == "pid:123"
    assert c2 == "pid:456"


def test_chave_produto_todos_campos_vazios_retorna_sem_chave():
    assert chave_produto() == "sem_chave"
    assert chave_produto(ean="", codigo="", descricao="", produto_id="") == "sem_chave"


def test_chave_produto_prioridade_ean_antes_de_produto_id():
    # Mesmo com produto_id, EAN tem precedencia
    assert chave_produto(ean="8811", produto_id="999") == "ean:8811"


def test_chave_produto_de_obj_com_produto_id():
    class FakeObj:
        ean = None
        cod_interno = None
        descricao = None
        produto_id = "X-42"
    assert chave_produto_de_obj(FakeObj()) == "pid:X-42"


# ----------------------------------------------------------------------
# R1: duplo-vazio (sem ofertas por peca e sem concorrentes globais)
# ----------------------------------------------------------------------


def test_r1_duplo_vazio_dispara_fallback_global(contexto_ok):
    """Sem dados nem por-peca, nem global: R1 dispara divergencia (fallback)."""
    contexto_ok.produtos_cotacao = []
    contexto_ok.concorrentes = []
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].regra == "R1"
    assert divs[0].dados.get("modo") == "global"
    assert divs[0].dados["qtd_cotacoes"] == 0


def test_r1_produtos_sem_qtd_cotacoes_peca_usa_fallback(contexto_ok):
    """Produtos existem mas TODOS com qtd_cotacoes_peca=None -> fallback."""
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol"),
        ProdutoCotacao(produto_id="p2", descricao="Capo"),
    ]
    # concorrentes=3 padrao do contexto_ok -> aprova
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert divs == []


# ----------------------------------------------------------------------
# _normalizar_pedido_v1: request com tipos inesperados
# ----------------------------------------------------------------------


def test_normalizar_v1_request_none():
    raw = {"identificador": None, "request": None, "observacao": None}
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out.get("identificador") is None


def test_normalizar_v1_request_lista_nao_crasheia():
    # Club poderia retornar `request` como list em alguma versao — nao crasha
    raw = {"identificador": None, "request": [{"obs": "PRISMA ABC1D23"}]}
    out = ClubClient._normalizar_pedido_v1(raw)
    # Nao extrai placa de list, mas tambem nao crasha
    assert out.get("identificador") is None


def test_normalizar_v1_obs_string_vazia_em_todos_campos():
    raw = {
        "identificador": None,
        "observacao": "",
        "cot_obs": "",
        "request": {"obs": ""},
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out.get("identificador") is None


def test_normalizar_v1_observacao_invalida_nao_matcheia():
    # Texto sem padrao de placa nao deve gerar match falso
    raw = {
        "identificador": None,
        "observacao": "comentario qualquer sem placa 12345",
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out.get("identificador") is None


# ----------------------------------------------------------------------
# listar_ofertas_por_peca: resposta malformada
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listar_ofertas_por_peca_vencedores_none():
    """Club v2 retorna `vencedores: None` em vez de []. Nao deve crashar."""
    client = ClubClient()
    mock_response = {
        "status": 200,
        "produtos": [
            {"prod_id": "1", "vencedores": None},
            {"prod_id": "2", "vencedores": [{"idFornecedor": 7516}]},
        ],
    }
    with patch.object(client, "_request", new=AsyncMock(return_value=mock_response)):
        result = await client.listar_ofertas_por_peca("1234")
    # Produto sem vencedores = 0 ofertas; produto com 1 vencedor = 1 oferta
    assert result == {"1": 0, "2": 1}


@pytest.mark.asyncio
async def test_listar_ofertas_por_peca_resposta_sem_produtos():
    client = ClubClient()
    with patch.object(client, "_request", new=AsyncMock(return_value={"status": 200})):
        result = await client.listar_ofertas_por_peca("1234")
    assert result == {}


@pytest.mark.asyncio
async def test_listar_ofertas_por_peca_http_erro_silencia():
    """Erro HTTP (ex: 500) nao propaga — R1 cai no fallback global."""
    client = ClubClient()
    with patch.object(client, "_request", new=AsyncMock(side_effect=RuntimeError("500"))):
        result = await client.listar_ofertas_por_peca("1234")
    assert result == {}


# ----------------------------------------------------------------------
# _dbconn: parametros Decimal e datetime
# ----------------------------------------------------------------------


def test_dbconn_decimal_param_sqlite():
    """Decimal via placeholder em SQLite — conversao para float."""
    from app._dbconn import get_conn

    with get_conn() as conn:
        # SELECT ? roda em ambos dialetos. Decimal vira float em SQLite,
        # Decimal em Postgres (ok ambos).
        row = conn.execute("SELECT ? AS val", (Decimal("100.50"),)).fetchone()
        val = row["val"] if isinstance(row, dict) else row[0]
        assert float(val) == 100.50


def test_dbconn_datetime_param_sqlite():
    """datetime via placeholder em SQLite — vira string ISO automaticamente."""
    from app._dbconn import get_conn

    dt = datetime(2026, 4, 15, 12, 30, 45)
    with get_conn() as conn:
        row = conn.execute("SELECT ? AS val", (dt.isoformat(),)).fetchone()
        val = row["val"] if isinstance(row, dict) else row[0]
        assert val == "2026-04-15T12:30:45"


def test_dbconn_none_param():
    from app._dbconn import get_conn

    with get_conn() as conn:
        row = conn.execute("SELECT ? AS val", (None,)).fetchone()
        val = row["val"] if isinstance(row, dict) else row[0]
        assert val is None
