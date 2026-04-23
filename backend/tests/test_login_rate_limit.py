"""Rate limit em `get_current_user` (aplicado em /api/auth/me e cia).

Ataque testado:
  * 5+ falhas em 60s (mesmo IP+user)  → 429 no 6o
  * 20+ falhas em 60s (IP global, usernames diferentes) → 429
  * IP diferente nao herda contador de outro IP
  * Janela desliza (freezegun): apos 61s contador limpa
  * X-Forwarded-For respeitado (spoof de porta NAO afeta per-ip composto)
  * Sucesso *nao* conta negativamente (nao ativa rate limit)
  * 429 tem `Retry-After`
  * Disable flag (LOGIN_RATE_ENABLED=false) pula checagem
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from freezegun import freeze_time

from app import db as app_db
from app.config import settings
from app.main import app
from app.services import auth as auth_service

pytestmark = pytest.mark.skipif(
    settings.db_dialect == "postgres",
    reason="Tests usam tmp SQLite; em Postgres rodam via integracao.",
)


ADMIN = "admin_rl"
SENHA = "senha12345"
CLIENT_IP = "203.0.113.55"


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "rl.db"
    monkeypatch.setattr(
        type(settings), "db_full_path", property(lambda self: db_file),
    )
    app_db.init_db()
    perfil_id = app_db.criar_perfil("Admin", "Admin")
    app_db.criar_usuario(
        username=ADMIN, nome="Admin", senha_hash=auth_service.hash_senha(SENHA),
        perfil_id=perfil_id, email=None, must_change_password=False,
    )
    yield db_file


@pytest.fixture
def client(tmp_db):
    return TestClient(app)


def _me(client, user: str, password: str, ip: str = CLIENT_IP):
    """Helper: chama /auth/me com XFF simulando cliente."""
    return client.get(
        "/api/auth/me",
        auth=(user, password),
        headers={"X-Forwarded-For": ip},
    )


# ---------- 1: 6a tentativa falhada retorna 429 ----------

def test_6_falhas_seguidas_disparam_429(client):
    with freeze_time("2026-04-23 12:00:00"):
        for i in range(settings.login_rate_ip_user_max):
            r = _me(client, ADMIN, "senha_errada")
            assert r.status_code == 401, f"tentativa {i+1}: esperado 401, veio {r.status_code}"
        # 6a tentativa: rate limit
        r = _me(client, ADMIN, "senha_errada")
        assert r.status_code == 429
        assert "retry-after" in {k.lower() for k in r.headers.keys()}
        assert r.headers["Retry-After"] == str(settings.login_rate_janela_s)


# ---------- 2: janela desliza (apos 61s reseta) ----------

def test_apos_janela_contador_expira(client):
    with freeze_time("2026-04-23 12:00:00") as frozen:
        for _ in range(settings.login_rate_ip_user_max):
            _me(client, ADMIN, "senha_errada")
        r_429 = _me(client, ADMIN, "senha_errada")
        assert r_429.status_code == 429

        # Avanca janela + 1s → entradas antigas saem, contador = 0
        frozen.tick(delta=settings.login_rate_janela_s + 1)
        r_ok = _me(client, ADMIN, SENHA)
        assert r_ok.status_code == 200, r_ok.text


# ---------- 3: IP diferente nao herda contador ----------

def test_outro_ip_nao_eh_bloqueado(client):
    with freeze_time("2026-04-23 12:00:00"):
        for _ in range(settings.login_rate_ip_user_max + 1):
            _me(client, ADMIN, "senha_errada", ip="1.2.3.4")
        # IP novo: tentativa ainda valida credencial (401), sem 429
        r = _me(client, ADMIN, "senha_errada", ip="5.6.7.8")
        assert r.status_code == 401


# ---------- 4: limite global por IP (username-spray) ----------

def test_username_spray_dispara_limite_global_ip(client):
    """Atacante rotaciona usernames no mesmo IP — chave composta nao
    segura, mas limite global por IP vai. Ao atingir LOGIN_RATE_IP_MAX
    tentativas falhadas no mesmo IP com usuarios diferentes, 429."""
    with freeze_time("2026-04-23 12:00:00"):
        # Rotaciona 21 usernames diferentes — 21 > 20 (IP_MAX)
        limit = settings.login_rate_ip_max
        for i in range(limit):
            r = _me(client, f"usr{i}", "qualquer")
            assert r.status_code == 401
        # 21a tentativa (IP global ja tem 20 falhas): 429
        r = _me(client, "usr_diff", "qualquer")
        assert r.status_code == 429


# ---------- 5: sucesso nao dispara rate limit ----------

def test_logins_bem_sucedidos_nao_bloqueiam(client):
    with freeze_time("2026-04-23 12:00:00"):
        for _ in range(settings.login_rate_ip_user_max + 5):
            r = _me(client, ADMIN, SENHA)
            assert r.status_code == 200


# ---------- 6: Retry-After presente e igual a janela ----------

def test_429_tem_retry_after_igual_a_janela(client):
    with freeze_time("2026-04-23 12:00:00"):
        for _ in range(settings.login_rate_ip_user_max):
            _me(client, ADMIN, "senha_errada")
        r = _me(client, ADMIN, "senha_errada")
        assert r.status_code == 429
        assert r.headers.get("Retry-After") == str(settings.login_rate_janela_s)


# ---------- 7: LOGIN_RATE_ENABLED=false pula checagem ----------

def test_disable_flag_pula_rate_limit(client, monkeypatch):
    monkeypatch.setattr(settings, "login_rate_enabled", False)
    with freeze_time("2026-04-23 12:00:00"):
        # 10 falhas: mesmo assim nao dispara 429
        for _ in range(10):
            r = _me(client, ADMIN, "senha_errada")
            assert r.status_code == 401


# ---------- 8: unlock via CLI helper libera imediatamente ----------

def test_unlock_libera_usuario_bloqueado(client):
    from app.services.login_attempts import unlock
    with freeze_time("2026-04-23 12:00:00"):
        for _ in range(settings.login_rate_ip_user_max):
            _me(client, ADMIN, "senha_errada")
        assert _me(client, ADMIN, "senha_errada").status_code == 429

        # Unlock apaga falhas recentes
        removidos = unlock(username=ADMIN)
        assert removidos >= settings.login_rate_ip_user_max

        # Proxima tentativa com senha correta: 200
        r = _me(client, ADMIN, SENHA)
        assert r.status_code == 200, r.text
