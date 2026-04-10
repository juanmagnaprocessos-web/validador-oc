"""Testes de regressao para normalizacao de placa.

Bug historico: OrdemCompra.placa_normalizada nao removia espacos, enquanto
PipefyClient._normalizar_placa removia. Quando uma placa vinha do Club com
espaco (ex: "QQF 2C69"), o lookup em indice_cards_historicos dava miss, e
os alertas R2 cross-time eram suprimidos.

Alem disso, a mesma inconsistencia causava duplicacao visual no relatorio:
a mesma placa aparecia simultaneamente no dashboard (com card sem cotacao)
e na secao "Revisao Final" (com cotacao).
"""
from app.clients.pipefy_client import PipefyClient
from app.models import Fornecedor, OrdemCompra


def _oc(identificador: str) -> OrdemCompra:
    return OrdemCompra(
        id_pedido="1",
        id_cotacao="1",
        identificador=identificador,
        fornecedor=Fornecedor(
            for_id="1", for_nome="Teste", for_status="1", for_excluido="0"
        ),
        items=[],
    )


def test_placa_normalizada_remove_hifen():
    assert _oc("ABC-1234").placa_normalizada == "ABC1234"


def test_placa_normalizada_remove_espacos():
    """Regressao: placa com espaco agora deve colapsar igual ao Pipefy."""
    assert _oc("QQF 2C69").placa_normalizada == "QQF2C69"


def test_placa_normalizada_remove_hifen_e_espacos():
    assert _oc(" QQF-2C69 ").placa_normalizada == "QQF2C69"


def test_placa_normalizada_uppercase():
    assert _oc("qqf2c69").placa_normalizada == "QQF2C69"


def test_placa_normalizada_vazia():
    assert _oc("").placa_normalizada == ""


def test_paridade_com_pipefy_normalizar_placa():
    """A normalizacao do modelo deve bater 1:1 com a do PipefyClient.

    Essa paridade e o que garante que o lookup em indice_cards_historicos
    encontra o card pela placa vinda do Club (bug raiz do R2 ausente).
    """
    entradas = [
        "QQF 2C69",
        "QQF-2C69",
        "qqf 2c69",
        "  PAN-1D24 ",
        "abc1234",
    ]
    for e in entradas:
        oc_norm = _oc(e).placa_normalizada
        pipefy_norm = PipefyClient._normalizar_placa(e)
        assert oc_norm == pipefy_norm, (
            f"Divergencia para entrada {e!r}: "
            f"OrdemCompra={oc_norm!r} vs Pipefy={pipefy_norm!r}"
        )
