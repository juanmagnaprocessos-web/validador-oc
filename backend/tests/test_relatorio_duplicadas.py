"""Testes de listar_duplicadas_periodo (relatorio de duplicadas R2 por periodo)."""
from __future__ import annotations

import pytest

from app import db as app_db
from app.config import settings
from app.services.report import gerar_excel_duplicadas


pytestmark = pytest.mark.skipif(
    settings.db_dialect == "postgres",
    reason="Testes isolados em SQLite temp.",
)


def _div_cross(oc_anterior="90", devolucao_peca=False):
    return {
        "regra": "R2",
        "titulo": "Peça duplicada (cross-time): reincidência SEM devolução (mesmo fornecedor)",
        "severidade": "alerta",
        "dados": {
            "descricao_peca": "PARACHOQUE DIANTEIRO",
            "oc_anterior": oc_anterior,
            "data_anterior": "2026-03-01",
            "fornecedor_anterior_nome": "Forn A",
            "mesmo_fornecedor": True,
            "tem_devolucao_peca": devolucao_peca,
            "tem_devolucao_outra_peca": False,
        },
    }


def _div_intra():
    return {
        "regra": "R2",
        "titulo": "Peça duplicada: 1 item(ns)",
        "severidade": "alerta",
        "dados": {"duplicados": ["cod:77"]},
    }


def _div_qtd_cilia():
    return {"regra": "R2", "titulo": "Quantidade diverge do Cilia", "dados": {}}


def _registrar(data_d1, pedido, divs, dry_run=True):
    vid = app_db.registrar_validacao(
        data_d1=data_d1, total_ocs=1, aprovadas=0, divergentes=1,
        bloqueadas=0, dry_run=dry_run, executado_por="teste", origem="manual",
    )
    app_db.registrar_oc_resultado(vid, {
        "id_pedido": pedido, "placa": "ABC-1D23", "placa_normalizada": "ABC1D23",
        "fornecedor": "Forn A", "status": "divergencia",
        "divergencias_json": divs,
    })
    return vid


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "dup_test.db"
    monkeypatch.setattr(
        type(settings), "db_full_path", property(lambda self: db_file)
    )
    app_db.init_db()
    _registrar("2026-04-10", "100", [_div_cross(), _div_qtd_cilia()])
    _registrar("2026-04-11", "200", [_div_intra()])
    _registrar("2026-04-20", "300", [_div_cross(devolucao_peca=True)])
    return db_file


def test_sem_filtro_traz_so_duplicadas_r2(tmp_db):
    linhas = app_db.listar_duplicadas_periodo()
    assert sorted(l["id_pedido"] for l in linhas) == ["100", "200", "300"]


def test_filtro_de_data(tmp_db):
    linhas = app_db.listar_duplicadas_periodo("2026-04-11", "2026-04-15")
    assert [l["id_pedido"] for l in linhas] == ["200"]
    assert linhas[0]["tipo"] == "Repetida na cotação"
    assert linhas[0]["peca"] == "cod:77"


def test_campos_da_reincidencia(tmp_db):
    (l,) = app_db.listar_duplicadas_periodo("2026-04-10", "2026-04-10")
    assert l["peca"] == "PARACHOQUE DIANTEIRO"
    assert l["oc_anterior"] == "90"
    assert l["devolucao"] == "Sem devolução"
    assert l["mesmo_fornecedor"] is True


def test_devolucao_da_peca(tmp_db):
    (l,) = app_db.listar_duplicadas_periodo("2026-04-20", "2026-04-20")
    assert l["devolucao"] == "Com devolução da peça"


def test_revalidacao_do_mesmo_dia_nao_conta_em_dobro(tmp_db):
    _registrar("2026-04-10", "100", [_div_cross()], dry_run=False)
    linhas = app_db.listar_duplicadas_periodo("2026-04-10", "2026-04-10")
    assert len(linhas) == 1


def test_excel_gerado(tmp_db):
    conteudo = gerar_excel_duplicadas(app_db.listar_duplicadas_periodo(), None, None)
    assert conteudo[:2] == b"PK"  # xlsx e um zip
