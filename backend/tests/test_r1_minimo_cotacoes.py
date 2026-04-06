from app.models import Concorrente
from app.validators.r1_minimo_cotacoes import R1MinimoCotacoes


def test_r1_aprova_com_3_cotacoes(contexto_ok):
    assert R1MinimoCotacoes().validar(contexto_ok) == []


def test_r1_sinaliza_com_2_cotacoes(contexto_ok):
    contexto_ok.concorrentes = contexto_ok.concorrentes[:2]
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].regra == "R1"
    assert "2" in divs[0].titulo


def test_r1_sinaliza_com_zero(contexto_ok):
    contexto_ok.concorrentes = []
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].dados["qtd_cotacoes"] == 0


def test_r1_aprova_com_muitas(contexto_ok):
    contexto_ok.concorrentes = [
        Concorrente(id_fornecedor=str(i), fornecedor_nome=f"F{i}")
        for i in range(10)
    ]
    assert R1MinimoCotacoes().validar(contexto_ok) == []
