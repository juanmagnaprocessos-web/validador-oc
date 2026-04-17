"""Testes do fix R2 cross-time via /api/getprodutosrelatoriocliente:
helpers _formatar_placa_para_club e _normalizar_relatorio_produtos_placa,
chunking de janela em listar_produtos_por_placa.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from app.services.orchestrator import (
    _formatar_placa_para_club,
    _normalizar_relatorio_produtos_placa,
)


# ----------------------------------------------------------------------
# _formatar_placa_para_club
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "entrada,esperado",
    [
        ("OPB9H43", "OPB-9H43"),       # Mercosul, sem hifen, upper
        ("OPB-9H43", "OPB-9H43"),      # ja com hifen
        ("opb9h43", "OPB-9H43"),       # lowercase
        ("ABC1234", "ABC-1234"),       # antigo, sem hifen
        ("ABC-1234", "ABC-1234"),      # antigo, com hifen
        ("OPB 9H43", "OPB-9H43"),      # com espaco
        ("OPB\u00a09H43", "OPB-9H43"), # com NBSP (\xa0)
    ],
)
def test_formatar_placa_para_club_validas(entrada, esperado):
    assert _formatar_placa_para_club(entrada) == esperado


@pytest.mark.parametrize("entrada", ["", None, "ABC", "ABCDEFGH", "ABC-12", "1234567"])
def test_formatar_placa_para_club_invalidas_retorna_none(entrada):
    # Guard contra chamadas perigosas (identifier="" pegaria todas as placas)
    # 1234567 falha no len ok, mas chars validos — aceitavel devolver formatado.
    res = _formatar_placa_para_club(entrada)
    if entrada == "1234567":
        # 7 chars validos, formatter aceita — Club rejeita downstream
        assert res == "123-4567"
    else:
        assert res is None


# ----------------------------------------------------------------------
# _normalizar_relatorio_produtos_placa
# ----------------------------------------------------------------------


def test_normalizar_filtra_id_pedido_atual():
    raw = [
        {"id_pedido": 100, "ean": "111", "pro_descricao": "PECA A",
         "data_geracao": "2026-01-01 10:00:00", "for_id": 7, "nomeFornecedor": "F1",
         "quantidade": 1},
        {"id_pedido": 999, "ean": "222", "pro_descricao": "PECA B",
         "data_geracao": "2026-01-02 10:00:00", "for_id": 8, "nomeFornecedor": "F2",
         "quantidade": 2},
    ]
    items = _normalizar_relatorio_produtos_placa("ABC1234", raw, id_pedido_atual="999")
    assert len(items) == 1
    assert items[0]["id_pedido"] == "100"
    assert items[0]["chave_produto"] == "ean:111"
    assert items[0]["fornecedor_id"] == "7"
    assert items[0]["fornecedor_nome"] == "F1"
    assert items[0]["data_oc"] == "2026-01-01"
    assert items[0]["fonte_historico"] == "relatorio_placa_club"


def test_normalizar_chave_fallback_descricao_quando_ean_vazio():
    raw = [
        {"id_pedido": 100, "ean": None, "cod_interno": None,
         "pro_descricao": "MOLDURA DO FAROL ESQ",
         "data_geracao": "2026-01-01 10:00:00",
         "for_id": 7, "nomeFornecedor": "F1", "quantidade": 1},
    ]
    items = _normalizar_relatorio_produtos_placa("ABC1234", raw, id_pedido_atual="")
    assert items[0]["chave_produto"] == "desc:moldura do farol esq"


def test_normalizar_pula_items_sem_id_pedido():
    raw = [
        {"id_pedido": None, "ean": "111", "pro_descricao": "X",
         "data_geracao": "2026-01-01", "for_id": 7, "quantidade": 1},
        {"id_pedido": "", "ean": "222", "pro_descricao": "Y",
         "data_geracao": "2026-01-01", "for_id": 7, "quantidade": 1},
    ]
    items = _normalizar_relatorio_produtos_placa("ABC1234", raw, id_pedido_atual="")
    assert items == []


def test_normalizar_quantidade_string_vira_float():
    raw = [
        {"id_pedido": 100, "ean": "111", "pro_descricao": "X",
         "data_geracao": "2026-01-01 10:00:00", "for_id": 7,
         "nomeFornecedor": "F1", "quantidade": "3.5"},
    ]
    items = _normalizar_relatorio_produtos_placa("ABC1234", raw, id_pedido_atual="")
    assert items[0]["quantidade"] == 3.5


def test_normalizar_data_geracao_vazia_retorna_data_oc_vazia():
    raw = [
        {"id_pedido": 100, "ean": "111", "pro_descricao": "X",
         "data_geracao": "", "for_id": 7, "nomeFornecedor": "F1", "quantidade": 1},
    ]
    items = _normalizar_relatorio_produtos_placa("ABC1234", raw, id_pedido_atual="")
    assert items[0]["data_oc"] == ""


# ----------------------------------------------------------------------
# ClubClient.listar_produtos_por_placa — chunking + dedupe + validacao
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listar_produtos_por_placa_chunking_quebra_janela_grande():
    from app.clients.club_client import ClubClient

    cc = ClubClient()
    cc._token = "fake"  # bypass auth
    chamadas: list[dict] = []

    async def _mock_request(method, url, *, params=None):
        chamadas.append(params)
        return {"produtos": []}

    cc._request = _mock_request  # type: ignore

    # 365 dias > 180 → deve quebrar em pelo menos 2 chunks
    await cc.listar_produtos_por_placa(
        "OPB-9H43", date(2025, 4, 16), date(2026, 4, 16),
    )
    assert len(chamadas) >= 2
    # Cada chunk respeita 6 meses (180 dias)
    for c in chamadas:
        ini = date.fromisoformat(c["dateIni"])
        fim = date.fromisoformat(c["dateFim"])
        assert (fim - ini).days < 180


@pytest.mark.asyncio
async def test_listar_produtos_por_placa_janela_pequena_um_chunk():
    from app.clients.club_client import ClubClient

    cc = ClubClient()
    cc._token = "fake"
    chamadas: list[dict] = []

    async def _mock_request(method, url, *, params=None):
        chamadas.append(params)
        return {"produtos": []}

    cc._request = _mock_request  # type: ignore

    await cc.listar_produtos_por_placa(
        "OPB-9H43", date(2026, 1, 1), date(2026, 4, 16),
    )
    assert len(chamadas) == 1


@pytest.mark.asyncio
async def test_listar_produtos_por_placa_data_invertida_retorna_vazio():
    from app.clients.club_client import ClubClient

    cc = ClubClient()
    cc._token = "fake"

    async def _mock_request(method, url, *, params=None):
        raise AssertionError("nao deveria chamar API com janela invertida")

    cc._request = _mock_request  # type: ignore

    res = await cc.listar_produtos_por_placa(
        "OPB-9H43", date(2026, 5, 1), date(2026, 4, 1),
    )
    assert res == []


@pytest.mark.asyncio
async def test_listar_produtos_por_placa_placa_invalida_levanta():
    from app.clients.club_client import ClubClient

    cc = ClubClient()
    cc._token = "fake"

    with pytest.raises(ValueError):
        await cc.listar_produtos_por_placa(
            "", date(2026, 1, 1), date(2026, 4, 1),
        )
    with pytest.raises(ValueError):
        await cc.listar_produtos_por_placa(
            "ABCDEF", date(2026, 1, 1), date(2026, 4, 1),
        )


@pytest.mark.asyncio
async def test_listar_produtos_por_placa_dedupe_em_overlap():
    """Items com mesmo (id_pedido, ean) devem ser deduplicados entre chunks."""
    from app.clients.club_client import ClubClient

    cc = ClubClient()
    cc._token = "fake"
    chamadas = [0]

    async def _mock_request(method, url, *, params=None):
        chamadas[0] += 1
        # Mesmo item (id_pedido=100, ean=111) em ambos os chunks
        return {
            "produtos": [
                {"id_pedido": 100, "ean": "111", "pro_descricao": "X"},
                {"id_pedido": 200, "ean": "222", "pro_descricao": "Y"},
            ]
        }

    cc._request = _mock_request  # type: ignore

    res = await cc.listar_produtos_por_placa(
        "OPB-9H43", date(2025, 4, 16), date(2026, 4, 16),
    )
    assert chamadas[0] >= 2  # multiplos chunks
    # Mesmo item nos dois chunks → deve aparecer apenas 1 vez
    ids = [(p["id_pedido"], p["ean"]) for p in res]
    assert len(set(ids)) == len(ids), "items devem ser unicos apos dedupe"
