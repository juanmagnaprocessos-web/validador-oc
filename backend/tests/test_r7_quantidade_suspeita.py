from app.models import ProdutoCotacao, Severidade
from app.validators.r7_quantidade_suspeita import R7QuantidadeSuspeita


def test_r7_quantidade_1_nao_gera_divergencia(contexto_ok):
    # Fixture default tem Farol e Capô com quantidade=1
    assert R7QuantidadeSuspeita().validar(contexto_ok) == []


def test_r7_quantidade_2_gera_alerta(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(
            produto_id="p1",
            descricao="DEFLETOR/CONVERGEDOR",
            quantidade=2,
            ean="123",
        ),
    ]
    divs = R7QuantidadeSuspeita().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].regra == "R7"
    assert divs[0].severidade == Severidade.ALERTA
    assert "DEFLETOR/CONVERGEDOR" in divs[0].titulo
    assert divs[0].dados["quantidade"] == 2.0
    assert divs[0].dados["chave_produto"] == "ean:123"


def test_r7_quantidade_3_gera_alerta(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="OLEO MOTOR", quantidade=3),
    ]
    divs = R7QuantidadeSuspeita().validar(contexto_ok)
    assert len(divs) == 1
    assert "CILIA" in divs[0].descricao


def test_r7_multiplas_pecas_filtra_apenas_qtd_maior_1(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="FAROL", quantidade=1, ean="1"),
        ProdutoCotacao(produto_id="p2", descricao="OLEO", quantidade=4, ean="2"),
        ProdutoCotacao(produto_id="p3", descricao="CAPO", quantidade=1, ean="3"),
        ProdutoCotacao(produto_id="p4", descricao="FILTRO", quantidade=2, ean="4"),
    ]
    divs = R7QuantidadeSuspeita().validar(contexto_ok)
    assert len(divs) == 2
    descricoes = {d.dados["descricao_peca"] for d in divs}
    assert descricoes == {"OLEO", "FILTRO"}


def test_r7_quantidade_zero_ou_none_sem_divergencia(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="PECA_ZERO", quantidade=0),
    ]
    assert R7QuantidadeSuspeita().validar(contexto_ok) == []
