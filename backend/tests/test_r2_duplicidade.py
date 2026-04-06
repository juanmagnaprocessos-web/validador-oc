from decimal import Decimal

from app.models import ItemCilia, OrcamentoCilia, ProdutoCotacao, Severidade
from app.validators.r2_duplicidade import R2Duplicidade


def test_r2_aprova_sem_duplicidade(contexto_ok):
    assert R2Duplicidade().validar(contexto_ok) == []


def test_r2_detecta_duplicidade_por_ean(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol", quantidade=1, ean="111"),
        ProdutoCotacao(produto_id="p2", descricao="Farol dir", quantidade=1, ean="111"),
    ]
    divs = R2Duplicidade().validar(contexto_ok)
    assert any("duplicada" in d.titulo.lower() for d in divs)


def test_r2_detecta_duplicidade_por_descricao(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol dianteiro", quantidade=1),
        ProdutoCotacao(produto_id="p2", descricao="Farol dianteiro", quantidade=1),
    ]
    divs = R2Duplicidade().validar(contexto_ok)
    assert len(divs) >= 1


def test_r2_divergencia_quantidade_cilia_stub_vira_info(contexto_ok):
    # Com CILIA_MODE=stub (default nos testes), divergência vira INFO
    contexto_ok.orcamento_cilia = OrcamentoCilia(
        placa="PAN1D24",
        valor_total=Decimal("1500.00"),
        itens=[
            ItemCilia(descricao="X", quantidade=5),  # Club tem 2, Cilia tem 5
        ],
    )
    divs = R2Duplicidade().validar(contexto_ok)
    cilia_divs = [d for d in divs if "qtd" in d.titulo.lower()]
    assert cilia_divs
    assert all(d.severidade == Severidade.INFO for d in cilia_divs)
