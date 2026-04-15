"""Migra dados da producao (Render Free SQLite efemero) para o Neon.

Fonte: API de producao (https://validador-oc.onrender.com).
Destino: Neon (configurado via DATABASE_URL do ambiente).

Estrategia:
  1. GET /api/admin/usuarios                      -> migra usuarios extras
  2. GET /api/historico?limite=100                -> pega todas validacoes
  3. GET /api/validacoes/{id}/resultados          -> resultados de cada
  4. Insere no Neon preservando IDs, ajusta sequences

Seguro para rodar varias vezes (idempotente via ON CONFLICT DO NOTHING
nos INSERTs). Nao migra senha_hash (nao exposta via API); usuarios extras
recebem senha temporaria nova.

Uso:
    # Garantir DATABASE_URL aponta para Neon
    export DATABASE_URL='postgresql://...neon...'
    cd backend
    python scripts/migrate_prod_to_neon.py \\
        --prod-url https://validador-oc.onrender.com \\
        --admin-user admin \\
        --admin-pass admin123
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets
import string
import sys
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app._dbconn import get_conn  # noqa: E402
from app.config import settings  # noqa: E402
from app.logging_setup import get_logger  # noqa: E402
from app.services.auth import hash_senha  # noqa: E402

logger = get_logger(__name__)


def gerar_senha_temp() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(12))


def fetch_prod(base: str, user: str, pwd: str, path: str) -> Any:
    url = f"{base.rstrip('/')}{path}"
    r = httpx.get(url, auth=(user, pwd), timeout=60.0)
    r.raise_for_status()
    return r.json()


def migrar_usuarios(conn, prod_usuarios: list[dict]) -> dict[str, str]:
    """Cria usuarios que nao existam no Neon. Retorna {username: senha_temp}."""
    senhas_criadas: dict[str, str] = {}
    # Pega perfil Admin (ja criado pelo seed)
    row = conn.execute("SELECT id FROM perfis WHERE nome = ?", ("Admin",)).fetchone()
    if not row:
        raise RuntimeError("Perfil Admin nao encontrado no Neon (rode init_db antes)")
    perfil_admin_id = row["id"] if isinstance(row, dict) else row[0]

    for u in prod_usuarios:
        uname = u["username"]
        existe = conn.execute(
            "SELECT 1 FROM usuarios WHERE LOWER(username) = LOWER(?)", (uname,)
        ).fetchone()
        if existe:
            logger.info("Usuario ja existe no Neon: %s", uname)
            continue
        senha_temp = gerar_senha_temp()
        conn.execute(
            """INSERT INTO usuarios
               (username, nome, email, senha_hash, perfil_id, ativo,
                must_change_password, criado_em)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                uname,
                u.get("nome") or uname,
                u.get("email"),
                hash_senha(senha_temp),
                perfil_admin_id,
                1 if u.get("ativo", True) else 0,
                1,  # must_change_password: sempre TRUE (senha gerada)
                u.get("criado_em"),
            ),
        )
        senhas_criadas[uname] = senha_temp
        logger.info("Usuario migrado: %s (senha temp gerada)", uname)
    conn.commit()
    return senhas_criadas


def migrar_validacoes(conn, validacoes: list[dict]) -> None:
    for v in validacoes:
        existe = conn.execute(
            "SELECT 1 FROM validacoes WHERE id = ?", (v["id"],)
        ).fetchone()
        if existe:
            logger.info("Validacao %s ja existe no Neon, pulando", v["id"])
            continue
        conn.execute(
            """INSERT INTO validacoes
               (id, data_execucao, data_d1, total_ocs, aprovadas, divergentes,
                bloqueadas, status, dry_run, relatorio_html, relatorio_xlsx,
                executado_por, aguardando_ml, ja_processadas)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                v["id"],
                v.get("data_execucao"),
                v.get("data_d1"),
                v.get("total_ocs", 0),
                v.get("aprovadas", 0),
                v.get("divergentes", 0),
                v.get("bloqueadas", 0),
                v.get("status", "pendente_revisao"),
                v.get("dry_run", 1),
                v.get("relatorio_html"),
                v.get("relatorio_xlsx"),
                v.get("executado_por"),
                v.get("aguardando_ml", 0),
                v.get("ja_processadas", 0),
            ),
        )
        logger.info("Validacao migrada: id=%s data_d1=%s", v["id"], v.get("data_d1"))
    conn.commit()
    # setval e feito de forma global em _ajustar_sequences() no final


def migrar_resultados(conn, validacao_id: int, resultados: list[dict]) -> None:
    for r in resultados:
        existe = conn.execute(
            "SELECT 1 FROM oc_resultados WHERE id = ?", (r["id"],)
        ).fetchone()
        if existe:
            continue
        regras = r.get("regras_falhadas")
        if isinstance(regras, (list, dict)):
            regras = json.dumps(regras, ensure_ascii=False)
        divergencias_json = r.get("divergencias_json")
        if isinstance(divergencias_json, (list, dict)):
            divergencias_json = json.dumps(divergencias_json, ensure_ascii=False, default=str)
        produtos_json = r.get("produtos_json")
        if isinstance(produtos_json, (list, dict)):
            produtos_json = json.dumps(produtos_json, ensure_ascii=False, default=str)
        conn.execute(
            """INSERT INTO oc_resultados
               (id, validacao_id, id_pedido, id_cotacao, placa, placa_normalizada,
                fornecedor, comprador, forma_pagamento, valor_card, valor_club,
                valor_pdf, valor_cilia, qtd_cotacoes, qtd_produtos, peca_duplicada,
                status, regras_falhadas, fase_pipefy, fase_pipefy_atual,
                card_pipefy_id, divergencias_json, produtos_json, reincidencia,
                cancelamento, cancelamento_card_id, card_pipefy_link,
                forma_pagamento_canonica)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                r["id"], r["validacao_id"], r.get("id_pedido"), r.get("id_cotacao"),
                r.get("placa"), r.get("placa_normalizada"),
                r.get("fornecedor"), r.get("comprador"), r.get("forma_pagamento"),
                r.get("valor_card"), r.get("valor_club"),
                r.get("valor_pdf"), r.get("valor_cilia"),
                r.get("qtd_cotacoes"), r.get("qtd_produtos"),
                r.get("peca_duplicada"), r.get("status"),
                regras, r.get("fase_pipefy"), r.get("fase_pipefy_atual"),
                r.get("card_pipefy_id"), divergencias_json, produtos_json,
                r.get("reincidencia"), r.get("cancelamento"),
                r.get("cancelamento_card_id"), r.get("card_pipefy_link"),
                r.get("forma_pagamento_canonica"),
            ),
        )
    conn.commit()
    logger.info("Migrados %d resultados da validacao %s", len(resultados), validacao_id)
    # setval global em _ajustar_sequences() no final


def _ajustar_sequences(conn) -> None:
    """Ajusta sequences (BIGSERIAL) apos migracao com IDs explicitos.

    Evita o bug classico: insere com id=1..50 explicitamente mas a sequence
    continua em 1 -> proximo INSERT natural colide com id ja existente.
    Chama `setval` com MAX(id) REAL da tabela (robusto mesmo se migrar
    varias vezes em ordens diferentes).

    So faz sentido em Postgres (SQLite nao precisa).
    """
    if settings.db_dialect != "postgres":
        return
    tabelas = [
        ("validacoes", "validacoes_id_seq"),
        ("oc_resultados", "oc_resultados_id_seq"),
        ("auditoria_api", "auditoria_api_id_seq"),
        ("acoes_pipefy_planejadas", "acoes_pipefy_planejadas_id_seq"),
        ("usuarios", "usuarios_id_seq"),
        ("perfis", "perfis_id_seq"),
    ]
    for tabela, seq in tabelas:
        row = conn.execute(f"SELECT COALESCE(MAX(id), 0) AS m FROM {tabela}").fetchone()
        max_id = row["m"] if isinstance(row, dict) else row[0]
        if max_id and int(max_id) > 0:
            conn.execute(f"SELECT setval('{seq}', {int(max_id)}, true)")
            logger.info("Sequence ajustada: %s -> %s", seq, max_id)
    conn.commit()


def _mascarar_url(url: str) -> str:
    """Mascara credenciais em uma URL Postgres para log/print.

    postgresql://user:pass@host/db?... -> postgresql://user:***@host/db?...
    """
    if "@" not in url:
        return url
    prefix, resto = url.split("@", 1)
    if ":" in prefix:
        esquema_user, _ = prefix.rsplit(":", 1)
        return f"{esquema_user}:***@{resto}"
    return url


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod-url", default="https://validador-oc.onrender.com")
    ap.add_argument("--admin-user", default="admin")
    ap.add_argument(
        "--admin-pass",
        default=None,
        help=(
            "Senha do admin de prod. Se omitida, le de PROD_ADMIN_PASS env "
            "ou pede interativamente (preferido — nao vaza em shell history)."
        ),
    )
    args = ap.parse_args()

    if settings.db_dialect != "postgres":
        print("ERRO: DATABASE_URL nao aponta para Postgres. Abortando.", file=sys.stderr)
        return 1

    # Resolve senha: CLI arg -> env -> prompt
    admin_pass = admin_pass or os.getenv("PROD_ADMIN_PASS")
    if not admin_pass:
        admin_pass = getpass.getpass(f"Senha admin de prod ({args.admin_user}): ")
    if not admin_pass:
        print("ERRO: senha admin vazia. Abortando.", file=sys.stderr)
        return 1

    print(f"Fonte: {args.prod_url}")
    print(f"Destino: Neon ({_mascarar_url(settings.database_url)})")
    print()

    # 1. Usuarios
    print(">> Baixando usuarios de prod...")
    usuarios = fetch_prod(args.prod_url, args.admin_user, admin_pass, "/api/admin/usuarios")
    print(f"   {len(usuarios)} usuarios")

    # 2. Validacoes
    print(">> Baixando historico...")
    validacoes = fetch_prod(args.prod_url, args.admin_user, admin_pass, "/api/historico?limite=100")
    print(f"   {len(validacoes)} validacoes")

    # 3. Resultados de cada validacao
    resultados_por_val: dict[int, list[dict]] = {}
    for v in validacoes:
        vid = v["id"]
        rs = fetch_prod(args.prod_url, args.admin_user, admin_pass,
                        f"/api/validacoes/{vid}/resultados")
        resultados_por_val[vid] = rs
        print(f"   validacao {vid}: {len(rs)} resultados")

    # 4. Inserir no Neon
    print()
    print(">> Migrando para Neon...")
    with get_conn() as conn:
        senhas = migrar_usuarios(conn, usuarios)
        migrar_validacoes(conn, validacoes)
        for vid, rs in resultados_por_val.items():
            migrar_resultados(conn, vid, rs)
        _ajustar_sequences(conn)

    print()
    print("=== MIGRACAO CONCLUIDA ===")
    print(f"Validacoes: {len(validacoes)}")
    print(f"Resultados: {sum(len(rs) for rs in resultados_por_val.values())}")
    print(f"Usuarios novos: {len(senhas)}")
    if senhas:
        print()
        print(">>> SENHAS TEMPORARIAS (repasse aos usuarios) <<<")
        for uname, senha in senhas.items():
            print(f"    {uname}: {senha}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
