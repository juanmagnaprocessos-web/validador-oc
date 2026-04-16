from app.models import ProdutoCotacao, Severidade
from app.validators import REGRAS_PADRAO, aplicar_regras
from app.validators.r7_quantidade_suspeita import R7QuantidadeSuspeita


def test_r7_quantidade_1_nao_gera_divergencia(contexto_ok):
    # Fixture default tem Farol e Capô com quantidade=1
    assert R7QuantidadeSuspeita().validar(contexto_ok) == []


def test_r7_quantidade_2_gera_info(contexto_ok):
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
    assert divs[0].severidade == Severidade.INFO
    assert "DEFLETOR/CONVERGEDOR" in divs[0].titulo
    assert divs[0].dados["quantidade"] == 2.0
    assert divs[0].dados["chave_produto"] == "ean:123"


def test_r7_quantidade_3_gera_info(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="OLEO MOTOR", quantidade=3),
    ]
    divs = R7QuantidadeSuspeita().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].severidade == Severidade.INFO
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


def test_r7_nao_bloqueia_oc_com_qtd_maior_1(contexto_ok):
    """Integração: OC válida em tudo, mas com uma peça qtd=3.

    Regressão do risco detectado no code review: se R7 fosse ALERTA,
    esta OC seria movida para 'Informações Incorretas'. Com INFO, não há
    bloqueante e o orchestrator mantém status APROVADA.
    """
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(
            produto_id="p1", descricao="FAROL", quantidade=1,
            ean="1", qtd_cotacoes_peca=3,
        ),
        ProdutoCotacao(
            produto_id="p2", descricao="OLEO MOTOR", quantidade=3,
            ean="2", qtd_cotacoes_peca=3,
        ),
    ]
    divs = aplicar_regras(REGRAS_PADRAO, contexto_ok)
    divs_r7 = [d for d in divs if d.regra == "R7"]
    assert len(divs_r7) == 1
    assert divs_r7[0].severidade == Severidade.INFO
    bloqueantes = [
        d for d in divs
        if d.severidade in (Severidade.ERRO, Severidade.ALERTA)
    ]
    assert bloqueantes == [], f"Bloqueantes inesperados: {[d.titulo for d in bloqueantes]}"


def test_r7_convive_com_r2_duplicidade(contexto_ok):
    """Mesma peça em 2 linhas + uma das linhas com qtd>1 gera R2 + R7."""
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(
            produto_id="p1", descricao="OLEO", quantidade=3,
            ean="X", qtd_cotacoes_peca=3,
        ),
        ProdutoCotacao(
            produto_id="p2", descricao="OLEO", quantidade=1,
            ean="X", qtd_cotacoes_peca=3,
        ),
    ]
    divs = aplicar_regras(REGRAS_PADRAO, contexto_ok)
    assert any(d.regra == "R2" for d in divs)
    divs_r7 = [d for d in divs if d.regra == "R7"]
    assert len(divs_r7) == 1
    assert divs_r7[0].dados["quantidade"] == 3.0
