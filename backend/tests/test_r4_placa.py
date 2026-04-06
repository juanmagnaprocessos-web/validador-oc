from app.validators.r4_placa import R4Placa


def test_r4_aceita_placa_mercosul(contexto_ok):
    # PAN1D24 é placa Mercosul válida
    assert R4Placa().validar(contexto_ok) == []


def test_r4_aceita_placa_antiga(contexto_ok):
    contexto_ok.oc.identificador = "ABC-1234"
    contexto_ok.card_pipefy.title = "ABC1234"
    assert R4Placa().validar(contexto_ok) == []


def test_r4_rejeita_formato_invalido(contexto_ok):
    contexto_ok.oc.identificador = "XXXX-99"
    divs = R4Placa().validar(contexto_ok)
    assert any("formato" in d.titulo.lower() for d in divs)


def test_r4_detecta_divergencia_com_card(contexto_ok):
    contexto_ok.oc.identificador = "PAN-1D24"
    contexto_ok.card_pipefy.title = "XYZ1D24"  # divergente
    divs = R4Placa().validar(contexto_ok)
    assert any("card" in d.titulo.lower() or "pipefy" in d.titulo.lower() for d in divs)


def test_r4_placa_ausente(contexto_ok):
    contexto_ok.oc.identificador = ""
    divs = R4Placa().validar(contexto_ok)
    assert len(divs) == 1
    assert "ausente" in divs[0].titulo.lower()
