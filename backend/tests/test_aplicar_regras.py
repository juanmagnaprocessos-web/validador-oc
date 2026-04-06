"""Teste de integração das regras via aplicar_regras."""
from datetime import date
from decimal import Decimal

from app.models import Concorrente, ContextoValidacao, Fornecedor, OrdemCompra
from app.validators import REGRAS_PADRAO, aplicar_regras


def test_oc_totalmente_valida_passa_por_todas_regras(contexto_ok):
    # Fixture já vem com anexo + PDF extraído = valor_club
    divs = aplicar_regras(REGRAS_PADRAO, contexto_ok)
    assert divs == []


def test_oc_com_multiplos_problemas():
    # OC com fornecedor inativo, placa estranha, sem cotações, data errada
    oc = OrdemCompra(
        id_pedido="999",
        id_cotacao="111",
        identificador="XXX",  # formato inválido
        valor_pedido=Decimal("1000.00"),
        fornecedor=Fornecedor(
            for_id="1", for_nome="X", for_status="0", for_excluido="0"
        ),
        data_pedido=date(2026, 4, 1),
    )
    ctx = ContextoValidacao(
        oc=oc,
        concorrentes=[Concorrente(id_fornecedor="1")],
        produtos_cotacao=[],
        data_d1=date(2026, 4, 5),
    )
    divs = aplicar_regras(REGRAS_PADRAO, ctx)
    regras_falhadas = {d.regra for d in divs}
    # Deve falhar em R1 (1<3), R4 (placa), R5 (inativo), R6 (data)
    assert "R1" in regras_falhadas
    assert "R4" in regras_falhadas
    assert "R5" in regras_falhadas
    assert "R6" in regras_falhadas
