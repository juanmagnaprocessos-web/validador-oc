from decimal import Decimal

from app.models import OrcamentoCilia, Severidade
from app.validators.r3_valor import R3Valor


def test_r3_aprova_quando_valores_batem(contexto_ok):
    # Fixture já tem anexo + PDF = valor_club
    divs = R3Valor().validar(contexto_ok)
    assert divs == []


def test_r3_erro_quando_card_ausente(contexto_ok):
    contexto_ok.card_pipefy = None
    divs = R3Valor().validar(contexto_ok)
    assert any("sem card" in d.titulo.lower() for d in divs)
    assert all(d.severidade == Severidade.ERRO for d in divs)


def test_r3_erro_quando_anexo_ausente(contexto_ok):
    contexto_ok.card_pipefy.anexo_oc_url = None
    contexto_ok.card_pipefy.valor_extraido_pdf = None
    divs = R3Valor().validar(contexto_ok)
    assert any("anexo" in d.titulo.lower() for d in divs)


def test_r3_alerta_quando_anexo_existe_mas_pdf_nao_parseavel(contexto_ok):
    contexto_ok.card_pipefy.valor_extraido_pdf = None
    divs = R3Valor().validar(contexto_ok)
    # Anexo existe (fixture), mas sem valor extraído → alerta
    assert any(d.severidade == Severidade.ALERTA for d in divs)


def test_r3_sinaliza_divergencia_club_vs_pdf(contexto_ok):
    contexto_ok.card_pipefy.valor_extraido_pdf = Decimal("1400.00")
    divs = R3Valor().validar(contexto_ok)
    assert any("PDF" in d.titulo for d in divs)


def test_r3_cilia_stub_vira_info(contexto_ok):
    # Com CILIA_MODE=stub (default nos testes), divergência vs Cilia é INFO
    contexto_ok.orcamento_cilia = OrcamentoCilia(
        placa="PAN1D24", valor_total=Decimal("1800.00")
    )
    divs = R3Valor().validar(contexto_ok)
    cilia_divs = [d for d in divs if "Cilia" in d.titulo]
    assert cilia_divs
    assert all(d.severidade == Severidade.INFO for d in cilia_divs)


def test_r3_classifica_magnitude(contexto_ok):
    contexto_ok.card_pipefy.valor_extraido_pdf = Decimal("1499.50")  # 50 centavos
    divs = R3Valor().validar(contexto_ok)
    assert any("centavos" in d.titulo.lower() for d in divs)


def test_r3_detecta_divergencia_campo_valor_card(contexto_ok):
    # PDF ok, mas campo Valor do card errado
    contexto_ok.card_pipefy.valor_card = Decimal("1200.00")
    divs = R3Valor().validar(contexto_ok)
    assert any("campo Valor" in d.titulo for d in divs)
