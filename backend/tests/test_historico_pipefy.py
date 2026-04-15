"""Testes unitarios para `_buscar_historico_placa_pipefy`.

A funcao e o nucleo do R2 cross-time apos a migracao do backfill SQLite
para o fluxo on-demand via Pipefy + Club. O bug do PR #1 (NameError
`descartadas_por_status`) nao foi pego pela suite porque nenhum teste
exercitava esse caminho — esta suite cobre essa lacuna.

Cenarios cobertos:
  1. Placa vazia -> `[]` (guarda precoce).
  2. Indice sem cards para a placa -> `[]`.
  3. Cards com `codigo_oc` vazio ou so-espaco -> filtrados.
  4. Card com `codigo_oc == id_pedido_atual` -> excluido (nao comparar consigo).
  5. `get_order_details` lanca -> item omitido, fluxo continua.
  6. Caminho feliz: 2 cards validos -> 2+ items com `chave_produto` populada.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import CardPipefy
from app.services.orchestrator import _buscar_historico_placa_pipefy


def _card(card_id: str, placa: str, codigo_oc: str | None, dias_atras: int = 10) -> CardPipefy:
    created = datetime.now(timezone.utc).replace(microsecond=0)
    created = created.fromordinal(created.toordinal() - dias_atras).replace(tzinfo=timezone.utc)
    return CardPipefy(
        id=card_id,
        title=placa,
        codigo_oc=codigo_oc,
        created_at=created,
    )


def _detalhes_club(id_pedido: str, items: list[dict]) -> dict:
    return {
        "id_pedido": id_pedido,
        "id_cotacao": f"COT-{id_pedido}",
        "fornecedor": {"for_id": "F1", "for_nome": "Fornecedor Teste"},
        "items": items,
    }


@pytest.mark.asyncio
async def test_placa_vazia_retorna_vazio():
    pipefy = MagicMock()
    club = MagicMock()

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="123",
        pipefy=pipefy,
    )
    assert result == []


@pytest.mark.asyncio
async def test_indice_sem_cards_para_a_placa():
    pipefy = MagicMock()
    pipefy.buscar_cards_por_placa = AsyncMock(return_value=[])
    club = MagicMock()

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="ABC1D23",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="999",
        pipefy=pipefy,
    )
    assert result == []
    pipefy.buscar_cards_por_placa.assert_awaited_once()


@pytest.mark.asyncio
async def test_cards_com_codigo_oc_vazio_sao_filtrados():
    # 3 cards: um com codigo vazio, um com so-espaco, um valido.
    cards = [
        _card("c1", "ABC1D23", codigo_oc=None),
        _card("c2", "ABC1D23", codigo_oc="   "),
        _card("c3", "ABC1D23", codigo_oc="555"),
    ]
    pipefy = MagicMock()
    pipefy.buscar_cards_por_placa = AsyncMock(return_value=cards)

    club = MagicMock()
    club.get_order_details = AsyncMock(
        return_value=_detalhes_club(
            "555",
            [{"product": {"ean": "8811", "name": "Parachoque"}, "quantity": 1}],
        ),
    )

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="ABC1D23",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="999",
        pipefy=pipefy,
    )
    # Apenas o card valido deve ter gerado chamada ao Club.
    assert club.get_order_details.await_count == 1
    club.get_order_details.assert_awaited_with("555")
    assert len(result) == 1
    assert result[0]["id_pedido"] == "555"


@pytest.mark.asyncio
async def test_card_com_id_pedido_atual_e_excluido():
    # Card historico cujo codigo_oc coincide com a OC sendo validada agora:
    # deve ser excluido para nao se comparar consigo mesma.
    cards = [
        _card("c1", "ABC1D23", codigo_oc="999"),  # = id_pedido_atual
        _card("c2", "ABC1D23", codigo_oc="100"),
    ]
    pipefy = MagicMock()
    pipefy.buscar_cards_por_placa = AsyncMock(return_value=cards)

    club = MagicMock()
    club.get_order_details = AsyncMock(
        return_value=_detalhes_club(
            "100",
            [{"product": {"ean": "8811", "name": "Farol Esq"}, "quantity": 1}],
        ),
    )

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="ABC1D23",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="999",
        pipefy=pipefy,
    )
    # Soh o card 100 deve ter sido consultado.
    assert club.get_order_details.await_count == 1
    club.get_order_details.assert_awaited_with("100")
    assert all(item["id_pedido"] != "999" for item in result)


@pytest.mark.asyncio
async def test_get_order_details_lanca_nao_propaga():
    # Se o Club falha (timeout, 500, 404 inesperado), o card deve ser
    # omitido silenciosamente mas o fluxo global continua para os demais.
    cards = [
        _card("c1", "ABC1D23", codigo_oc="777"),
        _card("c2", "ABC1D23", codigo_oc="888"),
    ]
    pipefy = MagicMock()
    pipefy.buscar_cards_por_placa = AsyncMock(return_value=cards)

    async def _get(oc_id: str):
        if oc_id == "777":
            raise RuntimeError("Club API indisponivel")
        return _detalhes_club(
            oc_id,
            [{"product": {"ean": "8811", "name": "Capo"}, "quantity": 1}],
        )

    club = MagicMock()
    club.get_order_details = AsyncMock(side_effect=_get)

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="ABC1D23",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="999",
        pipefy=pipefy,
    )
    # Apenas o card 888 deve ter entrado no resultado.
    ids = {item["id_pedido"] for item in result}
    assert ids == {"888"}


@pytest.mark.asyncio
async def test_caminho_feliz_retorna_items_indexaveis():
    cards = [
        _card("c1", "ABC1D23", codigo_oc="111"),
        _card("c2", "ABC1D23", codigo_oc="222"),
    ]
    pipefy = MagicMock()
    pipefy.buscar_cards_por_placa = AsyncMock(return_value=cards)

    details = {
        "111": _detalhes_club(
            "111",
            [
                {"product": {"ean": "1000", "name": "Farol Direito"}, "quantity": 1},
                {"product": {"ean": "1001", "name": "Farol Esquerdo"}, "quantity": 1},
            ],
        ),
        "222": _detalhes_club(
            "222",
            [{"product": {"ean": "2000", "name": "Capo"}, "quantity": 2}],
        ),
    }
    club = MagicMock()
    club.get_order_details = AsyncMock(side_effect=lambda oc: details[oc])

    result = await _buscar_historico_placa_pipefy(
        placa_normalizada="ABC1D23",
        indice_cards={},
        club=club,
        data_max=date.today(),
        dias_max=210,
        id_pedido_atual="999",
        pipefy=pipefy,
    )
    # 3 items no total (2 do card 111 + 1 do card 222).
    assert len(result) == 3
    # Todos devem ter chave_produto populada e placa_normalizada.
    for item in result:
        assert item["chave_produto"]
        assert item["placa_normalizada"] == "ABC1D23"
        assert item["identificador"] == "ABC1D23"
        assert item["fornecedor_id"] == "F1"
        assert item["fornecedor_nome"] == "Fornecedor Teste"
    # IDs dos pedidos historicos preservados.
    ids = {item["id_pedido"] for item in result}
    assert ids == {"111", "222"}
