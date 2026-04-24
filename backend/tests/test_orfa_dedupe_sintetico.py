"""Dedupe defensivo de OCs orfas: so deve disparar quando existe card
SINTETICO para a placa, nunca quando a placa aparece em cards reais.

Cenarios:
  - QQF2C69: card sintetico (OC nao encontrada no Club) + 1 OC orfa da
    mesma placa no Club. Dedupe DEVE disparar (bug 2026-04-10).
  - HOE-6G23: 2 cotacoes distintas, ambas com cards reais, 1 OC orfa sem
    card. Dedupe NAO pode disparar (bug 2026-04-23, OC 2056011 sumindo).
"""
from __future__ import annotations

from app.models import OrdemCompra
from app.services.orchestrator import (
    ColetaOC,
    _placas_com_card_sintetico,
)


def _oc(id_pedido: str, placa: str) -> OrdemCompra:
    return OrdemCompra(id_pedido=id_pedido, identificador=placa)


def _coleta(id_pedido: str, placa: str) -> ColetaOC:
    return ColetaOC(
        oc=_oc(id_pedido, placa),
        concorrentes=[],
        produtos_cotacao=[],
        orcamento_cilia=None,
        card_pipefy=None,
    )


def test_card_sintetico_entra_na_set_para_dedupe():
    """Card sintetico (codigo_oc vazio -> id_pedido='card:XYZ') entra."""
    ocs_index = {"2001111": {}}
    coletas = [_coleta("card:abc123", "QQF-2C69")]

    placas = _placas_com_card_sintetico(coletas, ocs_index)

    assert placas == {"QQF2C69"}


def test_card_real_nao_entra_na_set():
    """Card com id_pedido valido do Club NUNCA entra (evita descartar
    orfa legitima de outra cotacao na mesma placa)."""
    ocs_index = {"2056038": {}, "2056011": {}}
    coletas = [_coleta("2056038", "HOE-6G23")]

    placas = _placas_com_card_sintetico(coletas, ocs_index)

    assert placas == set()


def test_mistura_real_e_sintetico_so_inclui_placa_do_sintetico():
    ocs_index = {"2056038": {}, "2001111": {}}
    coletas = [
        _coleta("2056038", "HOE-6G23"),
        _coleta("card:zzz", "QQF-2C69"),
        _coleta("9999999", "PXR-2G50"),   # codigo_oc invalido (nao em ocs_index)
    ]

    placas = _placas_com_card_sintetico(coletas, ocs_index)

    assert placas == {"QQF2C69", "PXR2G50"}


def test_placa_vazia_e_ignorada():
    ocs_index = {}
    coletas = [_coleta("card:sem-placa", "")]

    placas = _placas_com_card_sintetico(coletas, ocs_index)

    assert placas == set()
