"""Testes do CRON runner: computo D-1, lock persistente, early-exit.

Não tocam em APIs externas (Club/Pipefy) — mocks nos clients.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
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
    db_file = tmp_path / "cron_test.db"
    monkeypatch.setattr(
        type(settings),
        "db_full_path",
        property(lambda self: db_file),
    )
    app_db.init_db()
    return db_file


# ----------------------------------------------------------------------
# _computar_data_d1: timezone-correctness
# ----------------------------------------------------------------------


def test_computar_data_d1_em_brt_retorna_ontem():
    from app.services.cron_runner import _computar_data_d1

    # 2026-04-15 02:00 BRT → D-1 = 2026-04-14
    agora = datetime(2026, 4, 15, 2, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert _computar_data_d1(agora) == date(2026, 4, 14)


def test_computar_data_d1_com_utc_converte_para_brt():
    from app.services.cron_runner import _computar_data_d1

    # 2026-04-15 04:30 UTC = 2026-04-15 01:30 BRT → D-1 = 2026-04-14
    agora_utc = datetime(2026, 4, 15, 4, 30, tzinfo=ZoneInfo("UTC"))
    assert _computar_data_d1(agora_utc) == date(2026, 4, 14)


def test_computar_data_d1_meia_noite_virada():
    from app.services.cron_runner import _computar_data_d1

    # 2026-04-15 00:30 BRT → D-1 = 2026-04-14 (ainda é "ontem")
    agora = datetime(2026, 4, 15, 0, 30, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert _computar_data_d1(agora) == date(2026, 4, 14)


# ----------------------------------------------------------------------
# Lock persistente: aquisição, conflito, TTL
# ----------------------------------------------------------------------


def test_lock_primeira_aquisicao_sucede(tmp_db):
    assert app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=7200) is True


def test_lock_duplo_rodando_falha(tmp_db):
    assert app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=7200) is True
    # Mesmo dia, outro processo tenta: bloqueado
    assert app_db.adquirir_cron_lock("2026-04-14", "host:2", ttl_seconds=7200) is False


def test_lock_sucesso_bloqueia_nova_aquisicao_no_mesmo_ttl(tmp_db):
    app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-14", "sucesso")
    # Dia já processado → não deve sobrescrever
    assert app_db.adquirir_cron_lock("2026-04-14", "host:2", ttl_seconds=7200) is False


def test_lock_falha_libera_para_retry_imediato(tmp_db):
    app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-14", "falha", last_error="bum")
    # Próxima execução pode re-adquirir (falha libera)
    assert app_db.adquirir_cron_lock("2026-04-14", "host:2", ttl_seconds=7200, tentativa=2) is True


def test_lock_ttl_expirado_libera(tmp_db):
    app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=1)
    # Força expiração manipulando expires_at
    with app_db.get_conn() as conn:
        passado = (datetime.now() - timedelta(hours=3)).isoformat(timespec="seconds")
        conn.execute(
            "UPDATE cron_locks SET expires_at = ? WHERE data_d1 = ?",
            (passado, "2026-04-14"),
        )
        conn.commit()
    # Após TTL expirado, novo processo pode retomar
    assert app_db.adquirir_cron_lock("2026-04-14", "host:2", ttl_seconds=7200) is True


def test_lock_concorrente_so_um_adquire(tmp_db):
    """Simula 10 processos tentando adquirir lock simultaneamente.

    Com INSERT ... ON CONFLICT ... WHERE atômico, exatamente 1 adquire na
    primeira rodada. Os outros 9 falham. Se o lock tivesse race TOCTOU,
    múltiplos passariam.
    """
    from concurrent.futures import ThreadPoolExecutor

    def tentar(i: int) -> bool:
        return app_db.adquirir_cron_lock(
            "2026-04-14", f"host:{i}", ttl_seconds=7200, tentativa=1
        )

    with ThreadPoolExecutor(max_workers=10) as exe:
        resultados = list(exe.map(tentar, range(10)))

    # Exatamente 1 adquiriu — os outros viram a linha 'rodando' com TTL
    # válido e foram rejeitados pela WHERE clause do ON CONFLICT.
    assert sum(resultados) == 1, (
        f"Esperava 1 aquisição, obteve {sum(resultados)}: {resultados}"
    )


def test_ultimo_lock_e_ultima_falha(tmp_db):
    app_db.adquirir_cron_lock("2026-04-14", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-14", "sucesso")
    app_db.adquirir_cron_lock("2026-04-15", "host:1", ttl_seconds=7200)
    app_db.finalizar_cron_lock("2026-04-15", "falha", last_error="boom")

    ult = app_db.ultimo_cron_lock()
    assert ult is not None
    assert ult["data_d1"] == "2026-04-15"

    falha = app_db.ultima_falha_cron()
    assert falha is not None
    assert falha["data_d1"] == "2026-04-15"
    assert falha["last_error"] == "boom"


# ----------------------------------------------------------------------
# Probe de vazio: definição estrita (Club=0 AND Pipefy=0)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_vazio_com_club_vazio_e_pipefy_vazio(tmp_db):
    from app.services import cron_runner

    cc_mock = AsyncMock()
    cc_mock.__aenter__.return_value = cc_mock
    cc_mock.listar_pedidos = AsyncMock(return_value=[])

    pc_mock = AsyncMock()
    pc_mock.__aenter__.return_value = pc_mock
    pc_mock.listar_cards_fase = AsyncMock(return_value=[])

    with patch.object(cron_runner, "_probe_vazio", wraps=cron_runner._probe_vazio):
        with patch("app.clients.club_client.ClubClient", return_value=cc_mock), \
             patch("app.clients.pipefy_client.PipefyClient", return_value=pc_mock):
            vazio = await cron_runner._probe_vazio(date(2026, 4, 14))
    assert vazio is True


@pytest.mark.asyncio
async def test_probe_nao_vazio_se_club_positivo():
    """Club=5, Pipefy=0 NÃO deve ser vazio — é o bug que R3 deve pegar."""
    from app.services import cron_runner

    cc_mock = AsyncMock()
    cc_mock.__aenter__.return_value = cc_mock
    cc_mock.listar_pedidos = AsyncMock(return_value=[{"id": i} for i in range(5)])

    with patch("app.clients.club_client.ClubClient", return_value=cc_mock):
        vazio = await cron_runner._probe_vazio(date(2026, 4, 14))
    assert vazio is False


@pytest.mark.asyncio
async def test_probe_nao_vazio_se_pipefy_tem_cards_do_d1():
    """Club=0, Pipefy>0 no D-1 → NÃO é vazio."""
    from app.services import cron_runner

    data_d1 = date(2026, 4, 14)

    cc_mock = AsyncMock()
    cc_mock.__aenter__.return_value = cc_mock
    cc_mock.listar_pedidos = AsyncMock(return_value=[])

    card = MagicMock()
    card.created_at = datetime(2026, 4, 14, 10, 0)
    pc_mock = AsyncMock()
    pc_mock.__aenter__.return_value = pc_mock
    pc_mock.listar_cards_fase = AsyncMock(return_value=[card])

    with patch("app.clients.club_client.ClubClient", return_value=cc_mock), \
         patch("app.clients.pipefy_client.PipefyClient", return_value=pc_mock):
        vazio = await cron_runner._probe_vazio(data_d1)
    assert vazio is False
