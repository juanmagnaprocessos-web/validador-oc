"""Testes do ClubClient focados na interacao com o CircuitBreaker.

Bug alvo: cards do Pipefy com `codigo_oc` nao-numerico (ex: "MAGNA")
geravam chamadas GET /v3/api/clients/orders/MAGNA que retornavam 4xx
e eram contadas pelo breaker. Aos 5 cards-lixo, o breaker abria e
bloqueava todas as chamadas legitimas subsequentes (peças, cotações
concorrentes, relatório por placa) — explicando por que TODAS as OCs
apareciam com peças=0 e cotacoes=0 no dashboard.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.clients.club_client import (
    ClubAPIError,
    ClubClient,
    ClubNotFoundError,
)
from app.utils.circuit_breaker import CircuitState


# ----------------------------------------------------------------------
# Validacao pre-request: IDs nao-numericos rejeitados
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "id_invalido",
    ["MAGNA", "MAGNA-001", "abc", "", "  ", "123abc", "12.34"],
)
async def test_get_order_details_rejeita_id_nao_numerico(id_invalido):
    """IDs nao-numericos devem levantar ValueError ANTES de tocar a rede."""
    client = ClubClient()
    client._token = "fake-token"  # evita authenticate()

    # O metodo http nem deve ser chamado — validacao pre-request
    with patch.object(client, "_request", new=AsyncMock()) as mock_req:
        with pytest.raises(ValueError):
            await client.get_order_details(id_invalido)
        mock_req.assert_not_called()

    # Breaker intocado
    assert client._breaker.failures == 0
    assert client._breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_get_order_details_aceita_id_numerico():
    """IDs numericos (string ou int) devem passar a validacao."""
    client = ClubClient()
    client._token = "fake-token"

    fake_resp = {"id": 12345, "value": "100.00"}
    with patch.object(client, "_request", new=AsyncMock(return_value=fake_resp)):
        resp_str = await client.get_order_details("12345")
        resp_int = await client.get_order_details(12345)

    assert resp_str == fake_resp
    assert resp_int == fake_resp


@pytest.mark.asyncio
async def test_muitos_ids_invalidos_nao_abrem_breaker():
    """Cenario real: 10 cards com codigo_oc invalido em sequencia nao devem
    abrir o breaker nem impedir chamadas legitimas posteriores."""
    client = ClubClient()
    client._token = "fake-token"

    # 10 tentativas com IDs invalidos — todas devem levantar ValueError
    for i in range(10):
        with pytest.raises(ValueError):
            await client.get_order_details(f"MAGNA-{i:03d}")

    # Breaker ainda CLOSED
    assert client._breaker.state == CircuitState.CLOSED
    assert client._breaker.failures == 0

    # Chamada legitima posterior funciona normalmente
    fake_resp = {"id": 999, "value": "50.00"}
    with patch.object(client, "_request", new=AsyncMock(return_value=fake_resp)):
        r = await client.get_order_details("999")
    assert r == fake_resp


# ----------------------------------------------------------------------
# 404 do Club: ClubNotFoundError nao consome breaker
# ----------------------------------------------------------------------


def _make_fake_response(status_code: int, text: str = "") -> MagicMock:
    """Constroi um mock de httpx.Response minimamente util."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.json = MagicMock(return_value={})
    return resp


@pytest.mark.asyncio
async def test_404_levanta_club_not_found_error():
    """Status 404 deve gerar ClubNotFoundError (nao ClubAPIError generico)."""
    client = ClubClient()
    client._token = "fake-token"

    fake_404 = _make_fake_response(404, "Not Found")
    with patch.object(
        client._client, "request", new=AsyncMock(return_value=fake_404)
    ):
        with pytest.raises(ClubNotFoundError):
            await client.get_order_details("99999999")


@pytest.mark.asyncio
async def test_404_repetidos_nao_abrem_breaker():
    """Multiplos 404 em sequencia NAO devem abrir o breaker — 404 eh
    erro do cliente (id inexistente), nao sinal de Club fora do ar."""
    client = ClubClient()
    client._token = "fake-token"

    fake_404 = _make_fake_response(404, "Not Found")
    with patch.object(
        client._client, "request", new=AsyncMock(return_value=fake_404)
    ):
        for _ in range(10):
            with pytest.raises(ClubNotFoundError):
                await client.get_order_details("99999999")

    assert client._breaker.state == CircuitState.CLOSED
    assert client._breaker.failures == 0


@pytest.mark.asyncio
async def test_500_abre_breaker_normalmente():
    """Erros 5xx continuam contando e abrem o breaker (regressao)."""
    client = ClubClient()
    client._token = "fake-token"
    # Reduz retries para acelerar o teste
    import app.config as cfg
    original = cfg.settings.club_max_retries
    cfg.settings.club_max_retries = 1
    try:
        fake_500 = _make_fake_response(500, "Internal Error")
        with patch.object(
            client._client, "request", new=AsyncMock(return_value=fake_500)
        ):
            for _ in range(client._breaker.fail_threshold):
                with pytest.raises(ClubAPIError):
                    await client.get_order_details("12345")

        assert client._breaker.state == CircuitState.OPEN
    finally:
        cfg.settings.club_max_retries = original
