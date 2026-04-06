from app.models import Fornecedor
from app.validators.r5_fornecedor import R5Fornecedor


def test_r5_aprova_fornecedor_ativo(contexto_ok):
    assert R5Fornecedor().validar(contexto_ok) == []


def test_r5_rejeita_fornecedor_inativo(contexto_ok):
    contexto_ok.oc.fornecedor = Fornecedor(
        for_id="1", for_nome="Inativa", for_status="0", for_excluido="0"
    )
    divs = R5Fornecedor().validar(contexto_ok)
    assert any("inativo" in d.titulo.lower() for d in divs)


def test_r5_rejeita_fornecedor_excluido(contexto_ok):
    contexto_ok.oc.fornecedor = Fornecedor(
        for_id="1", for_nome="Excluída", for_status="1", for_excluido="1"
    )
    divs = R5Fornecedor().validar(contexto_ok)
    assert any("excluído" in d.titulo.lower() for d in divs)


def test_r5_fornecedor_ausente(contexto_ok):
    contexto_ok.oc.fornecedor = None
    divs = R5Fornecedor().validar(contexto_ok)
    assert len(divs) == 1
