"""Endpoints administrativos: gestão de usuários e perfis.

Todas as rotas exigem perfil Admin via `Depends(require_admin)`.
Endpoints de auth do próprio usuário (login info, troca de senha)
ficam em `/auth/*` e exigem apenas autenticação.
"""
from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status

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


# ---- Backfill de histórico de produtos ----
@admin_router.post("/backfill")
async def backfill_historico(
    dias: int = Query(210, ge=1, le=365, description="Janela de dias para backfill"),
    user: Usuario = Depends(require_admin),
):
    """Popula o histórico de produtos (OCs) baixando dados do Club.

    Usado na primeira execução ou para ampliar a janela de detecção de
    duplicatas (R2 cross-time). Roda FORA do timeout da validação —
    pode levar 15+ minutos para 210 dias na primeira vez.

    Requer perfil Admin.
    """
    from app.clients.club_client import ClubClient
    from app.db import dias_presentes_no_historico
    from app.logging_setup import get_logger
    from app.services.historico_produtos import garantir_historico

    logger = get_logger(__name__)

    data_d1 = date.today() - timedelta(days=1)
    inicio = data_d1 - timedelta(days=dias)

    # Info prévia: quantos dias já existem vs. total
    presentes_antes = dias_presentes_no_historico(
        inicio.isoformat(), data_d1.isoformat()
    )
    dias_faltantes = dias - len(presentes_antes)

    logger.info(
        "Backfill solicitado por %s: janela=%d dias, "
        "presentes=%d, faltantes=%d",
        user.username, dias, len(presentes_antes), dias_faltantes,
    )

    async with ClubClient() as club:
        # Budget generoso: o endpoint /backfill é explicitamente o lugar
        # onde o admin pode tomar o tempo necessário para popular a janela
        # inteira (rodado fora do timeout da validação).
        status_backfill = await garantir_historico(
            club,
            ate_dia=data_d1,
            dias_janela=dias,
            time_budget_seconds=60 * 30,  # 30 minutos
        )

    # Contagem pós-backfill
    presentes_depois = dias_presentes_no_historico(
        inicio.isoformat(), data_d1.isoformat()
    )

    return {
        "status": "ok",
        "janela_dias": dias,
        "periodo": f"{inicio.isoformat()} a {data_d1.isoformat()}",
        "dias_presentes_antes": len(presentes_antes),
        "dias_presentes_depois": len(presentes_depois),
        "dias_baixados": len(presentes_depois) - len(presentes_antes),
        "historico_status": status_backfill,
    }


@admin_router.get("/historico-status")
async def historico_status(user: Usuario = Depends(require_admin)):
    """Retorna o status atual do histórico de produtos (cobertura de dias).

    Usado para diagnóstico rápido: quantos dias da janela R2 estão
    populados, qual o primeiro e o último dia presentes. Se `completo`
    for False, há risco de alertas de duplicidade incompletos.
    """
    from datetime import date, timedelta

    from app.config import settings
    from app.db import get_conn

    data_max = date.today() - timedelta(days=1)
    data_min = data_max - timedelta(days=settings.r2_janela_dias)

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT data_oc FROM historico_produtos_oc "
            "WHERE data_oc BETWEEN ? AND ? ORDER BY data_oc",
            (data_min.isoformat(), data_max.isoformat()),
        ).fetchall()

    dias_cobertos = len(rows)
    primeiro = rows[0]["data_oc"] if rows else None
    ultimo = rows[-1]["data_oc"] if rows else None

    return {
        "dias_cobertos": dias_cobertos,
        "dias_necessarios": settings.r2_janela_dias,
        "completo": dias_cobertos >= settings.r2_janela_dias,
        "primeiro_dia_processado": primeiro,
        "ultimo_dia_processado": ultimo,
        "periodo_consultado": f"{data_min.isoformat()} a {data_max.isoformat()}",
    }
