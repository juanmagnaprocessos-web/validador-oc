from app.models import Concorrente, ProdutoCotacao
from app.validators.r1_minimo_cotacoes import R1MinimoCotacoes


# --------------------------------------------------------------------------
# Caminho fallback (global) — quando qtd_cotacoes_peca e None em todas as
# pecas. Esses testes existiam antes do endpoint por-peca e garantem que
# o fallback preserva o comportamento historico.
# --------------------------------------------------------------------------


def test_r1_aprova_com_3_cotacoes(contexto_ok):
    assert R1MinimoCotacoes().validar(contexto_ok) == []


def test_r1_sinaliza_com_2_cotacoes(contexto_ok):
    contexto_ok.concorrentes = contexto_ok.concorrentes[:2]
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    assert divs[0].regra == "R1"
    assert "2" in divs[0].titulo
    # Marca o modo para o caller conseguir distinguir
    assert divs[0].dados.get("modo") == "global"


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


# --------------------------------------------------------------------------
# Caminho por-peca — prioritario quando qualquer peca tem qtd_cotacoes_peca
# diferente de None (endpoint /api/v2/requests/{id}/products/offers
# respondeu). Valida minimo 3 POR PECA.
# --------------------------------------------------------------------------


def test_r1_por_peca_aprova_quando_todas_acima_do_minimo(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol", qtd_cotacoes_peca=4),
        ProdutoCotacao(produto_id="p2", descricao="Capo", qtd_cotacoes_peca=3),
        ProdutoCotacao(produto_id="p3", descricao="Parachoque", qtd_cotacoes_peca=7),
    ]
    # Mesmo se concorrentes globais < 3, o caminho por-peca tem precedencia.
    contexto_ok.concorrentes = []
    assert R1MinimoCotacoes().validar(contexto_ok) == []


def test_r1_por_peca_sinaliza_uma_peca_com_duas_cotacoes(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol", qtd_cotacoes_peca=4),
        ProdutoCotacao(produto_id="p2", descricao="Capo", qtd_cotacoes_peca=2),  # < 3
        ProdutoCotacao(produto_id="p3", descricao="Parachoque", qtd_cotacoes_peca=5),
    ]
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    d = divs[0]
    assert d.regra == "R1"
    insuficientes = d.dados["pecas_insuficientes"]
    assert len(insuficientes) == 1
    assert insuficientes[0]["descricao"] == "Capo"
    assert insuficientes[0]["qtd_cotacoes_peca"] == 2
    assert "modo" not in d.dados  # nao estamos no fallback global


def test_r1_por_peca_lista_multiplas_pecas_abaixo(contexto_ok):
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol DIR", qtd_cotacoes_peca=1),
        ProdutoCotacao(produto_id="p2", descricao="Farol ESQ", qtd_cotacoes_peca=2),
        ProdutoCotacao(produto_id="p3", descricao="Parachoque", qtd_cotacoes_peca=7),
    ]
    divs = R1MinimoCotacoes().validar(contexto_ok)
    assert len(divs) == 1
    insuficientes = divs[0].dados["pecas_insuficientes"]
    nomes = {p["descricao"] for p in insuficientes}
    assert nomes == {"Farol DIR", "Farol ESQ"}
    # Descricao da divergencia contem as pecas problema
    assert "Farol DIR" in divs[0].descricao
    assert "Farol ESQ" in divs[0].descricao


def test_r1_mistura_pecas_com_e_sem_dados_usa_caminho_por_peca(contexto_ok):
    # Pelo menos uma peca tem qtd_cotacoes_peca setado => caminho por-peca.
    # Pecas sem qtd_cotacoes_peca ficam fora da avaliacao (endpoint nao
    # retornou dados para elas, mas retornou para as outras — tratamos
    # como desconhecido e nao penalizamos).
    contexto_ok.produtos_cotacao = [
        ProdutoCotacao(produto_id="p1", descricao="Farol", qtd_cotacoes_peca=5),
        ProdutoCotacao(produto_id="p2", descricao="Capo", qtd_cotacoes_peca=None),
    ]
    contexto_ok.concorrentes = []  # garante que fallback global dispararia
    assert R1MinimoCotacoes().validar(contexto_ok) == []
