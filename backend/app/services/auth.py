"""HTTP Basic Auth + bcrypt + dependency injection.

Decisões tomadas com o usuário (registradas no plano):
  - Mecanismo: HTTPBasic (FastAPI nativo)
  - Hash: bcrypt direto (passlib foi descartado por incompatibilidade
          com bcrypt>=5.0). bcrypt limita a 72 bytes — truncamos a
          senha em UTF-8 com aviso silencioso.
  - Senha mínima: 8 chars
  - 1º login força troca de senha (`must_change_password=True`)
  - Sem JWT, sem lockout, sem auditoria de ações (Fase futura)
"""
from __future__ import annotations

import secrets

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.db import (
    get_perfil,
    get_usuario_por_username,
    registrar_login,
)
from app.logging_setup import get_logger
from app.models import Usuario

logger = get_logger(__name__)

_security = HTTPBasic(realm="Validador OC", auto_error=False)

_BCRYPT_MAX_BYTES = 72


def _to_bytes(senha: str) -> bytes:
    b = senha.encode("utf-8")
    if len(b) > _BCRYPT_MAX_BYTES:
        # bcrypt 5+ rejeita >72 bytes em vez de truncar. Truncamos em
        # boundary de bytes — pode cortar no meio de um caractere
        # multibyte, mas é determinístico (mesmo trunc no hash e na
        # verificação). Logamos para diagnóstico.
        logger.warning("Senha truncada para %d bytes (era %d).", _BCRYPT_MAX_BYTES, len(b))
        b = b[:_BCRYPT_MAX_BYTES]
    return b


def hash_senha(senha: str) -> str:
    return bcrypt.hashpw(_to_bytes(senha), bcrypt.gensalt()).decode("ascii")


def verificar_senha(senha: str, senha_hash: str) -> bool:
    try:
        return bcrypt.checkpw(_to_bytes(senha), senha_hash.encode("ascii"))
    except Exception:
        return False


def gerar_senha_temporaria(tamanho: int = 12) -> str:
    """Gera senha aleatória legível: 12 chars alfanuméricos."""
    alfabeto = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alfabeto) for _ in range(tamanho))


def _carregar_usuario_com_perfil(row: dict) -> Usuario:
    perfil = get_perfil(row["perfil_id"])
    return Usuario(
        id=row["id"],
        username=row["username"],
        nome=row["nome"],
        email=row.get("email"),
        perfil_id=row["perfil_id"],
        perfil_nome=perfil["nome"] if perfil else None,
        ativo=bool(row["ativo"]),
        must_change_password=bool(row["must_change_password"]),
        criado_em=row["criado_em"],
        ultimo_login=row.get("ultimo_login"),
    )


def get_current_user(
    credentials: HTTPBasicCredentials | None = Depends(_security),
) -> Usuario:
    """Dependency: valida Basic Auth e retorna o usuário autenticado.

    Usa auto_error=False para evitar que o navegador mostre o popup
    nativo de HTTP Basic Auth (WWW-Authenticate: Basic). O frontend
    gerencia o login via formulário proprio.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais não fornecidas",
        )

    row = get_usuario_por_username(credentials.username)
    if not row or not row.get("ativo"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais inválidas",
        )

    if not verificar_senha(credentials.password, row["senha_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais inválidas",
        )

    registrar_login(row["id"])
    return _carregar_usuario_com_perfil(row)


def require_admin(user: Usuario = Depends(get_current_user)) -> Usuario:
    """Dependency: além de autenticado, exige perfil 'Admin'.

    Hoje todos os usuários do sistema são Admin (única role criada no
    bootstrap). A checagem fica aqui para preparar a expansão futura
    de perfis sem mexer nos routers.
    """
    if (user.perfil_nome or "").lower() != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Requer perfil Admin",
        )
    return user
