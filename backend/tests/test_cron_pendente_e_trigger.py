"""Testes da deteccao 'CRON nao executou' e do endpoint trigger externo."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from app import db as app_db
from app.config import settings


pytestmark = pytest.mark.skipif(
    settings.db_dialect == "postgres",
    reason="Testes isolados em SQLite temp.",
)


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "cron_pendente_test.db"
    monkeypatch.setattr(
        type(settings), "db_full_path",
        property(lambda self: db_file),
    )
    monkeypatch.setattr(settings, "cron_enabled", True)
    monkeypatch.setattr(settings, "cron_hour_brt", 2)
    monkeypatch.setattr(settings, "cron_minute", 0)
    monkeypatch.setattr(settings, "cron_timezone", "America/Sao_Paulo")
    app_db.init_db()
    return db_file


# ----------------------------------------------------------------------
# cron_pendente_de_execucao
# ----------------------------------------------------------------------


def _patch_now(monkeypatch, dt: datetime):
    """Patch datetime.now() em app.db para retornar dt fixo."""
    real_dt = __import__("datetime").datetime

    class _FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return dt if tz is None else dt.astimezone(tz)

    monkeypatch.setattr("app.db.datetime", _FakeDT)


def test_cron_pendente_disabled_retorna_none(tmp_db, monkeypatch):
    monkeypatch.setattr(settings, "cron_enabled", False)
    assert app_db.cron_pendente_de_execucao() is None


def test_cron_pendente_antes_da_hora_retorna_none(tmp_db, monkeypatch):
    tz = ZoneInfo("America/Sao_Paulo")
    # 01:30 BRT — antes do horario CRON 02:00
    _patch_now(monkeypatch, datetime(2026, 4, 17, 1, 30, tzinfo=tz))
    assert app_db.cron_pendente_de_execucao() is None


def test_cron_pendente_apos_hora_sem_registro_retorna_dict(tmp_db, monkeypatch):
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    res = app_db.cron_pendente_de_execucao()
    assert res is not None
    assert res["data_d1"] == "2026-04-16"
    assert res["horario_esperado"] == "02:00"


def test_cron_pendente_com_registro_sucesso_retorna_none(tmp_db, monkeypatch):
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    app_db.adquirir_cron_lock("2026-04-16", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-16", "sucesso")
    assert app_db.cron_pendente_de_execucao() is None


def test_cron_pendente_com_registro_falha_retorna_none(tmp_db, monkeypatch):
    """Status 'falha' eh terminal — outro banner (ultima_falha) cobre."""
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    app_db.adquirir_cron_lock("2026-04-16", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-16", "falha", last_error="boom")
    assert app_db.cron_pendente_de_execucao() is None


def test_cron_pendente_com_lock_rodando_valido_retorna_none(tmp_db, monkeypatch):
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    # Lock 'rodando' com TTL ainda no futuro — execução em andamento
    app_db.adquirir_cron_lock("2026-04-16", "host:1", ttl_seconds=14400)
    assert app_db.cron_pendente_de_execucao() is None


def test_cron_pendente_com_lock_rodando_expirado_retorna_dict(tmp_db, monkeypatch):
    """Lock travado em 'rodando' com expires_at < now NAO conta como valido."""
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    # Lock criado no passado, expirado
    app_db.adquirir_cron_lock("2026-04-16", "host:1", ttl_seconds=1)
    # Forca expires_at para o passado
    with app_db.get_conn() as conn:
        passado = (datetime.now() - timedelta(hours=5)).isoformat(timespec="seconds")
        conn.execute(
            "UPDATE cron_locks SET expires_at = ? WHERE data_d1 = ?",
            (passado, "2026-04-16"),
        )
        conn.commit()
    res = app_db.cron_pendente_de_execucao()
    assert res is not None
    assert res["data_d1"] == "2026-04-16"


def test_cron_pendente_com_validacao_manual_retorna_none(tmp_db, monkeypatch):
    """Se usuario clicou 'Validar agora' (escreve em validacoes, nao em
    cron_locks), banner nao deve aparecer."""
    tz = ZoneInfo("America/Sao_Paulo")
    _patch_now(monkeypatch, datetime(2026, 4, 17, 8, 0, tzinfo=tz))
    # Insere validacao manual sem cron_locks
    with app_db.get_conn() as conn:
        conn.execute(
            """INSERT INTO validacoes (data_d1, data_execucao, dry_run,
                status, total_ocs, aprovadas, divergentes, bloqueadas,
                executado_por)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("2026-04-16", datetime.now().isoformat(timespec="seconds"),
             0, "concluido", 5, 4, 1, 0, "juanpablo"),
        )
        conn.commit()
    assert app_db.cron_pendente_de_execucao() is None


# ----------------------------------------------------------------------
# Endpoint POST /api/cron/trigger
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_sem_token_configurado_retorna_503(tmp_db, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(settings, "cron_trigger_token", "")
    from app.main import app
    with TestClient(app) as cli:
        resp = cli.post("/api/cron/trigger", headers={"X-Cron-Token": "qualquer"})
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_trigger_sem_header_retorna_403(tmp_db, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(settings, "cron_trigger_token", "secret-abc")
    from app.main import app
    with TestClient(app) as cli:
        resp = cli.post("/api/cron/trigger")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_trigger_token_errado_retorna_403(tmp_db, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(settings, "cron_trigger_token", "secret-abc")
    from app.main import app
    with TestClient(app) as cli:
        resp = cli.post("/api/cron/trigger", headers={"X-Cron-Token": "errado"})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_trigger_token_correto_dispara(tmp_db, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(settings, "cron_trigger_token", "secret-abc")

    async def _fake_job(data_d1_override=None):
        return None

    with patch("app.services.cron_runner.run_daily_validation_job", new=_fake_job):
        from app.main import app
        with TestClient(app) as cli:
            resp = cli.post(
                "/api/cron/trigger", headers={"X-Cron-Token": "secret-abc"}
            )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("status") in ("disparado", "ja_em_execucao")
