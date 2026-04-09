"""Testes mínimos da camada de autenticação e administração.

Cobertura intencional (mínimo viável, conforme decidido com o usuário):
  - HTTP Basic em /auth/me (sem credencial, inválido, válido)
  - Troca de senha no 1º login (must_change_password)
  - Bloqueio de "nova == atual"
  - require_admin: usuário comum recebe 403
  - Criar usuário + reset de senha + login com a nova
  - Admin não pode se inativar
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import db as app_db
from app.config import settings
from app.main import app
from app.services import auth as auth_service


ADMIN_USER = "admin_test"
ADMIN_PASS = "admin12345"  # >= 8 chars
COMUM_USER = "comum_test"
COMUM_PASS = "comum12345"


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Aponta o banco para um SQLite temporário, inicializa schema e
    semeia perfis Admin/Comum + um usuário admin e um comum prontos
    para login.

    Pydantic v2 bloqueia setattr direto em property no instance, então
    substituímos a property na própria classe Settings via monkeypatch
    (revertido automaticamente ao fim do teste).
    """
    db_file = tmp_path / "validador_test.db"
    monkeypatch.setattr(
        type(settings),
        "db_full_path",
        property(lambda self: db_file),
    )

    app_db.init_db()

    perfil_admin_id = app_db.criar_perfil("Admin", "Administradores")
    perfil_comum_id = app_db.criar_perfil("Comum", "Usuário comum")

    admin_id = app_db.criar_usuario(
        username=ADMIN_USER,
        nome="Admin de Teste",
        senha_hash=auth_service.hash_senha(ADMIN_PASS),
        perfil_id=perfil_admin_id,
        email="admin@test.local",
        must_change_password=True,
    )
    comum_id = app_db.criar_usuario(
        username=COMUM_USER,
        nome="Comum de Teste",
        senha_hash=auth_service.hash_senha(COMUM_PASS),
        perfil_id=perfil_comum_id,
        email="comum@test.local",
        must_change_password=False,
    )

    yield {
        "db_file": db_file,
        "perfil_admin_id": perfil_admin_id,
        "perfil_comum_id": perfil_comum_id,
        "admin_id": admin_id,
        "comum_id": comum_id,
    }


@pytest.fixture
def client(tmp_db):
    # TestClient sem context manager → não dispara lifespan,
    # evitando um init_db() redundante (idempotente, mas desnecessário).
    return TestClient(app)


# ---------- /auth/me ----------

def test_login_sem_credenciais_401(client):
    r = client.get("/auth/me")
    assert r.status_code == 401
    assert r.headers.get("www-authenticate", "").lower().startswith("basic")


def test_login_credenciais_invalidas_401(client):
    r = client.get("/auth/me", auth=(ADMIN_USER, "senha_errada"))
    assert r.status_code == 401


def test_login_ok_retorna_me(client):
    r = client.get("/auth/me", auth=(ADMIN_USER, ADMIN_PASS))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["username"] == ADMIN_USER
    assert body["perfil_nome"] == "Admin"
    assert body["must_change_password"] is True
    assert body["ativo"] is True


# ---------- /auth/trocar-senha ----------

def test_trocar_senha_obrigatoria_no_primeiro_login(client):
    nova = "novaSenha123"
    r = client.post(
        "/auth/trocar-senha",
        json={"senha_atual": ADMIN_PASS, "nova_senha": nova},
        auth=(ADMIN_USER, ADMIN_PASS),
    )
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True

    # Senha velha não funciona mais
    r_velha = client.get("/auth/me", auth=(ADMIN_USER, ADMIN_PASS))
    assert r_velha.status_code == 401

    # Senha nova funciona e flag de troca foi limpa
    r_nova = client.get("/auth/me", auth=(ADMIN_USER, nova))
    assert r_nova.status_code == 200
    assert r_nova.json()["must_change_password"] is False


def test_trocar_senha_nova_igual_atual_400(client):
    r = client.post(
        "/auth/trocar-senha",
        json={"senha_atual": ADMIN_PASS, "nova_senha": ADMIN_PASS},
        auth=(ADMIN_USER, ADMIN_PASS),
    )
    assert r.status_code == 400


# ---------- /admin/usuarios ----------

def test_admin_lista_usuarios_so_admin(client):
    # Admin: ok
    r_ok = client.get("/admin/usuarios", auth=(ADMIN_USER, ADMIN_PASS))
    assert r_ok.status_code == 200
    usernames = {u["username"] for u in r_ok.json()}
    assert ADMIN_USER in usernames and COMUM_USER in usernames

    # Usuário comum: 403
    r_403 = client.get("/admin/usuarios", auth=(COMUM_USER, COMUM_PASS))
    assert r_403.status_code == 403


def test_admin_cria_usuario_e_reseta_senha(client, tmp_db):
    # 1) cria usuário
    payload = {
        "username": "novo_user",
        "nome": "Novo Usuário",
        "email": "novo@test.local",
        "perfil_id": tmp_db["perfil_comum_id"],
        "senha_temporaria": "tempSenha1",
    }
    r_create = client.post(
        "/admin/usuarios", json=payload, auth=(ADMIN_USER, ADMIN_PASS)
    )
    assert r_create.status_code == 201, r_create.text
    novo = r_create.json()
    assert novo["username"] == "novo_user"
    assert novo["must_change_password"] is True

    # 2) login com a senha temporária funciona
    r_me = client.get("/auth/me", auth=("novo_user", "tempSenha1"))
    assert r_me.status_code == 200

    # 3) admin reseta a senha
    r_reset = client.post(
        f"/admin/usuarios/{novo['id']}/reset-senha",
        auth=(ADMIN_USER, ADMIN_PASS),
    )
    assert r_reset.status_code == 200, r_reset.text
    nova_temp = r_reset.json()["nova_senha_temporaria"]
    assert isinstance(nova_temp, str) and len(nova_temp) >= 8

    # 4) senha antiga não funciona; nova sim, e force-change está ligado
    assert client.get("/auth/me", auth=("novo_user", "tempSenha1")).status_code == 401
    r_pos = client.get("/auth/me", auth=("novo_user", nova_temp))
    assert r_pos.status_code == 200
    assert r_pos.json()["must_change_password"] is True


def test_admin_nao_pode_se_inativar(client, tmp_db):
    r = client.patch(
        f"/admin/usuarios/{tmp_db['admin_id']}",
        json={"ativo": False},
        auth=(ADMIN_USER, ADMIN_PASS),
    )
    assert r.status_code == 400

    # E o DELETE também é protegido
    r_del = client.delete(
        f"/admin/usuarios/{tmp_db['admin_id']}",
        auth=(ADMIN_USER, ADMIN_PASS),
    )
    assert r_del.status_code == 400
