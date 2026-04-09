"""Endpoints administrativos: gestão de usuários e perfis.

Todas as rotas exigem perfil Admin via `Depends(require_admin)`.
Endpoints de auth do próprio usuário (login info, troca de senha)
ficam em `/auth/*` e exigem apenas autenticação.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.db import (
    atualizar_perfil,
    atualizar_senha_usuario,
    atualizar_usuario,
    criar_perfil,
    criar_usuario,
    get_perfil,
    get_perfil_por_nome,
    get_usuario,
    get_usuario_por_username,
    listar_perfis,
    listar_usuarios,
)
from app.models import (
    Perfil,
    PerfilCreate,
    PerfilUpdate,
    ResetSenhaResponse,
    TrocarSenhaRequest,
    Usuario,
    UsuarioCreate,
    UsuarioUpdate,
)
from app.services.auth import (
    gerar_senha_temporaria,
    get_current_user,
    hash_senha,
    require_admin,
    verificar_senha,
)

# ----- /auth (qualquer usuário autenticado) -----
auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.get("/me", response_model=Usuario)
async def me(user: Usuario = Depends(get_current_user)):
    return user


@auth_router.post("/trocar-senha")
async def trocar_senha(
    payload: TrocarSenhaRequest,
    user: Usuario = Depends(get_current_user),
):
    row = get_usuario(user.id)
    if not row:
        raise HTTPException(404, "Usuário não encontrado")
    if not verificar_senha(payload.senha_atual, row["senha_hash"]):
        raise HTTPException(401, "Senha atual incorreta")
    if payload.senha_atual == payload.nova_senha:
        raise HTTPException(400, "A nova senha deve ser diferente da atual")
    atualizar_senha_usuario(
        user.id, hash_senha(payload.nova_senha), must_change_password=False
    )
    return {"ok": True, "mensagem": "Senha alterada com sucesso"}


# ----- /admin (somente perfil Admin) -----
admin_router = APIRouter(prefix="/admin", tags=["admin"])


# ---- Usuários ----
@admin_router.get("/usuarios", response_model=list[Usuario])
async def admin_listar_usuarios(_: Usuario = Depends(require_admin)):
    rows = listar_usuarios()
    return [
        Usuario(
            id=r["id"],
            username=r["username"],
            nome=r["nome"],
            email=r.get("email"),
            perfil_id=r["perfil_id"],
            perfil_nome=r.get("perfil_nome"),
            ativo=bool(r["ativo"]),
            must_change_password=bool(r["must_change_password"]),
            criado_em=r["criado_em"],
            ultimo_login=r.get("ultimo_login"),
        )
        for r in rows
    ]


@admin_router.post("/usuarios", response_model=Usuario, status_code=status.HTTP_201_CREATED)
async def admin_criar_usuario(
    payload: UsuarioCreate, _: Usuario = Depends(require_admin)
):
    if get_usuario_por_username(payload.username):
        raise HTTPException(409, f"Já existe usuário '{payload.username}'")
    if not get_perfil(payload.perfil_id):
        raise HTTPException(400, f"Perfil {payload.perfil_id} não existe")
    novo_id = criar_usuario(
        username=payload.username,
        nome=payload.nome,
        email=payload.email,
        senha_hash=hash_senha(payload.senha_temporaria),
        perfil_id=payload.perfil_id,
        must_change_password=True,
    )
    row = get_usuario(novo_id)
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


@admin_router.patch("/usuarios/{usuario_id}", response_model=Usuario)
async def admin_atualizar_usuario(
    usuario_id: int,
    payload: UsuarioUpdate,
    admin: Usuario = Depends(require_admin),
):
    row = get_usuario(usuario_id)
    if not row:
        raise HTTPException(404, "Usuário não encontrado")
    if payload.perfil_id is not None and not get_perfil(payload.perfil_id):
        raise HTTPException(400, f"Perfil {payload.perfil_id} não existe")
    if usuario_id == admin.id and payload.ativo is False:
        raise HTTPException(400, "Você não pode inativar a si mesmo")
    atualizar_usuario(
        usuario_id,
        nome=payload.nome,
        email=payload.email,
        perfil_id=payload.perfil_id,
        ativo=payload.ativo,
    )
    row = get_usuario(usuario_id)
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


@admin_router.post("/usuarios/{usuario_id}/reset-senha", response_model=ResetSenhaResponse)
async def admin_reset_senha(
    usuario_id: int, _: Usuario = Depends(require_admin)
):
    row = get_usuario(usuario_id)
    if not row:
        raise HTTPException(404, "Usuário não encontrado")
    nova = gerar_senha_temporaria()
    atualizar_senha_usuario(usuario_id, hash_senha(nova), must_change_password=True)
    return ResetSenhaResponse(nova_senha_temporaria=nova)


@admin_router.delete("/usuarios/{usuario_id}")
async def admin_inativar_usuario(
    usuario_id: int, admin: Usuario = Depends(require_admin)
):
    if usuario_id == admin.id:
        raise HTTPException(400, "Você não pode inativar a si mesmo")
    if not get_usuario(usuario_id):
        raise HTTPException(404, "Usuário não encontrado")
    atualizar_usuario(usuario_id, ativo=False)
    return {"ok": True, "mensagem": "Usuário inativado"}


# ---- Perfis ----
@admin_router.get("/perfis", response_model=list[Perfil])
async def admin_listar_perfis(_: Usuario = Depends(require_admin)):
    return [Perfil(**p) for p in listar_perfis()]


@admin_router.post("/perfis", response_model=Perfil, status_code=status.HTTP_201_CREATED)
async def admin_criar_perfil(
    payload: PerfilCreate, _: Usuario = Depends(require_admin)
):
    if get_perfil_por_nome(payload.nome):
        raise HTTPException(409, f"Já existe perfil '{payload.nome}'")
    novo_id = criar_perfil(payload.nome, payload.descricao, payload.permissoes)
    return Perfil(**get_perfil(novo_id))


@admin_router.patch("/perfis/{perfil_id}", response_model=Perfil)
async def admin_atualizar_perfil(
    perfil_id: int,
    payload: PerfilUpdate,
    _: Usuario = Depends(require_admin),
):
    if not get_perfil(perfil_id):
        raise HTTPException(404, "Perfil não encontrado")
    atualizar_perfil(
        perfil_id,
        nome=payload.nome,
        descricao=payload.descricao,
        permissoes=payload.permissoes,
    )
    return Perfil(**get_perfil(perfil_id))
