from datetime import date

from app.validators.r6_data import R6Data


def test_r6_aprova_d1_correto(contexto_ok):
    assert R6Data().validar(contexto_ok) == []


def test_r6_rejeita_data_diferente(contexto_ok):
    contexto_ok.oc.data_pedido = date(2026, 4, 4)  # D-1 no fixture é 2026-04-05
    divs = R6Data().validar(contexto_ok)
    assert len(divs) == 1
    assert "D-1" in divs[0].titulo


def test_r6_alerta_data_ausente(contexto_ok):
    contexto_ok.oc.data_pedido = None
    divs = R6Data().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].severidade.value == "alerta"
