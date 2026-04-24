"""Testa o log estruturado de tentativas de login e o endpoint admin.

Casos cobertos:
  * Cada resultado (sucesso, senha_errada, usuario_inexistente,
    usuario_desativado, credenciais_ausentes) e registrado corretamente
  * user_agent e truncado a 500 chars (anti DoS por log bloat)
  * GET /api/admin/login-attempts admin-only (403 pra nao-admin)
  * Filtros do endpoint admin (resultado, username, ip)
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import db as app_db
from app.config import settings
from app.main import app
from app.services import auth as auth_service

pytestmark = pytest.mark.skipif(
    settings.db_dialect == "postgres",
    reason="Tests usam tmp SQLite; em Postgres rodam via integracao.",
)

ADMIN = "admin_log"
ADMIN_SENHA = "senha12345"
COMUM = "comum_log"
COMUM_SENHA = "comum12345"


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "log.db"
    monkeypatch.setattr(
        type(settings), "db_full_path", property(lambda self: db_file),
    )
    # Rate limit off: queremos testar log, nao bloqueio
    monkeypatch.setattr(settings, "login_rate_enabled", False)

    app_db.init_db()
    perfil_admin = app_db.criar_perfil("Admin", "Admin")
    perfil_comum = app_db.criar_perfil("Comum", "Comum")
    app_db.criar_usuario(
        username=ADMIN, nome="A", senha_hash=auth_service.hash_senha(ADMIN_SENHA),
        perfil_id=perfil_admin, email=None, must_change_password=False,
    )
    comum_id = app_db.criar_usuario(
        username=COMUM, nome="C", senha_hash=auth_service.hash_senha(COMUM_SENHA),
        perfil_id=perfil_comum, email=None, must_change_password=False,
    )
    yield {"db_file": db_file, "comum_id": comum_id}


@pytest.fixture
def client(tmp_db):
    return TestClient(app)


def _listar(db_file, **filtros):
    from app.db import listar_tentativas_login
    return listar_tentativas_login(**filtros)


# ---------- 1: cada resultado e registrado com o label certo ----------

def test_registra_todos_os_resultados(client, tmp_db):
    # sucesso
    client.get("/api/auth/me", auth=(ADMIN, ADMIN_SENHA))
    # senha_errada
    client.get("/api/auth/me", auth=(ADMIN, "senha_errada_aqui"))
    # usuario_inexistente
    client.get("/api/auth/me", auth=("nao_existe", "qualquer"))
    # credenciais_ausentes (sem Authorization)
    client.get("/api/auth/me")

    # usuario_desativado: desativar o comum e tentar login
    app_db.atualizar_usuario(tmp_db["comum_id"], ativo=False)
    client.get("/api/auth/me", auth=(COMUM, COMUM_SENHA))

    rows = _listar(tmp_db["db_file"], limite=20)
    resultados = {r["resultado"] for r in rows}
    assert "sucesso" in resultados
    assert "senha_errada" in resultados
    assert "usuario_inexistente" in resultados
    assert "credenciais_ausentes" in resultados
    assert "usuario_desativado" in resultados


# ---------- 2: user_agent e truncado a 500 chars ----------

def test_user_agent_truncado(client, tmp_db):
    ua_gigante = "X" * 2000
    client.get(
        "/api/auth/me",
        auth=(ADMIN, "senha_qualquer"),
        headers={"User-Agent": ua_gigante},
    )
    rows = _listar(tmp_db["db_file"], limite=5)
    # A ultima tentativa registrada com username ADMIN
    for r in rows:
        if r["username"].lower() == ADMIN.lower() and r["user_agent"]:
            assert len(r["user_agent"]) <= 500
            return
    pytest.fail("tentativa nao foi registrada com user_agent")


# ---------- 3: endpoint admin requer Admin ----------

def test_endpoint_login_attempts_admin_only(client):
    # Admin: ok
    r_ok = client.get("/api/admin/login-attempts", auth=(ADMIN, ADMIN_SENHA))
    assert r_ok.status_code == 200
    # Comum: 403
    r_403 = client.get("/api/admin/login-attempts", auth=(COMUM, COMUM_SENHA))
    assert r_403.status_code == 403


# ---------- 4: filtros do endpoint admin funcionam ----------

def test_endpoint_login_attempts_filtros(client):
    # Gera variedade: 3 falhas senha + 1 sucesso
    for _ in range(3):
        client.get("/api/auth/me", auth=(ADMIN, "errada"))
    client.get("/api/auth/me", auth=(ADMIN, ADMIN_SENHA))

    # Sem filtro
    r_all = client.get(
        "/api/admin/login-attempts?limite=20",
        auth=(ADMIN, ADMIN_SENHA),
    ).json()
    assert r_all["total"] >= 4

    # So senha_errada
    r_f = client.get(
        "/api/admin/login-attempts?resultado=senha_errada",
        auth=(ADMIN, ADMIN_SENHA),
    ).json()
    assert r_f["total"] >= 3
    for t in r_f["tentativas"]:
        assert t["resultado"] == "senha_errada"

    # Username inexistente: vazio
    r_vazio = client.get(
        "/api/admin/login-attempts?username=ninguem_aqui",
        auth=(ADMIN, ADMIN_SENHA),
    ).json()
    assert r_vazio["total"] == 0
