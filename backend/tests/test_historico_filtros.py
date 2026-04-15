"""Testes de listar_historico com filtros de data_inicio/data_fim e origem."""
from __future__ import annotations

import pytest

from app import db as app_db
from app.config import settings


pytestmark = pytest.mark.skipif(
    settings.db_dialect == "postgres",
    reason="Testes isolados em SQLite temp.",
)


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "hist_test.db"
    monkeypatch.setattr(
        type(settings),
        "db_full_path",
        property(lambda self: db_file),
    )
    app_db.init_db()

    for data, origem in [
        ("2026-04-10", "manual"),
        ("2026-04-11", "cron"),
        ("2026-04-12", "manual"),
        ("2026-04-13", "cron"),
        ("2026-04-14", "cron"),
    ]:
        app_db.registrar_validacao(
            data_d1=data,
            total_ocs=10,
            aprovadas=5,
            divergentes=5,
            bloqueadas=0,
            dry_run=True,
            executado_por="teste",
            origem=origem,
        )
    return db_file


def test_listar_sem_filtros_retorna_tudo(tmp_db):
    rows = app_db.listar_historico(limite=100)
    assert len(rows) == 5


def test_filtro_data_inicio_inclusivo(tmp_db):
    rows = app_db.listar_historico(limite=100, data_inicio="2026-04-12")
    datas = sorted(r["data_d1"] for r in rows)
    assert datas == ["2026-04-12", "2026-04-13", "2026-04-14"]


def test_filtro_data_fim_inclusivo(tmp_db):
    rows = app_db.listar_historico(limite=100, data_fim="2026-04-12")
    datas = sorted(r["data_d1"] for r in rows)
    assert datas == ["2026-04-10", "2026-04-11", "2026-04-12"]


def test_filtro_range_fechado(tmp_db):
    rows = app_db.listar_historico(
        limite=100, data_inicio="2026-04-11", data_fim="2026-04-13"
    )
    datas = sorted(r["data_d1"] for r in rows)
    assert datas == ["2026-04-11", "2026-04-12", "2026-04-13"]


def test_limite_aplica_corte(tmp_db):
    rows = app_db.listar_historico(limite=2)
    assert len(rows) == 2


def test_origem_persistida(tmp_db):
    rows = app_db.listar_historico(limite=100)
    mp = {r["data_d1"]: r["origem"] for r in rows}
    assert mp["2026-04-10"] == "manual"
    assert mp["2026-04-11"] == "cron"
    assert mp["2026-04-14"] == "cron"
