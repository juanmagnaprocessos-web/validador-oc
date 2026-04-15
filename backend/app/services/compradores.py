"""Gestão da tabela auxiliar de compradores.

A API do Club expõe apenas o `created_by` (ID numérico) de quem criou a OC,
e um `usu_nome` que na prática contém o nome do fornecedor — não do comprador.

Para resolver o ID do comprador em nome+e-mail, mantemos uma tabela local
`compradores` em SQLite. Ela é populada manualmente pelo CLI
(`python -m app.cli compradores add ...`), ou importada de CSV.

Funções expostas:
- `init_table()` — cria a tabela se não existir
- `add(club_user_id, nome, email)` — upsert
- `remove(club_user_id)`
- `listar()` — lista todos
- `get(club_user_id)` — busca por ID
- `resolve(club_user_id)` — retorna (nome, email) com fallback heurístico
"""
from __future__ import annotations

from typing import Any

from app.db import get_conn
from app.logging_setup import get_logger

logger = get_logger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS compradores (
    club_user_id INTEGER PRIMARY KEY,
    nome         TEXT NOT NULL,
    email        TEXT NOT NULL,
    ativo        INTEGER NOT NULL DEFAULT 1,
    criado_em    TEXT DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def init_table() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def add(club_user_id: int, nome: str, email: str, ativo: bool = True) -> None:
    """Upsert de comprador.

    Passamos `criado_em` / `atualizado_em` explicitamente (em vez de
    depender de `DEFAULT CURRENT_TIMESTAMP`) para garantir formato ISO
    consistente entre SQLite e Postgres — `CURRENT_TIMESTAMP` em Postgres
    inclui fracoes de segundo e timezone, divergindo do formato usado no
    resto do sistema.
    """
    from datetime import datetime
    init_table()
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO compradores (club_user_id, nome, email, ativo,
                                        criado_em, atualizado_em)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(club_user_id) DO UPDATE SET
                   nome = excluded.nome,
                   email = excluded.email,
                   ativo = excluded.ativo,
                   atualizado_em = excluded.atualizado_em""",
            (
                int(club_user_id), nome.strip(), email.strip().lower(),
                1 if ativo else 0, agora, agora,
            ),
        )
        conn.commit()
    logger.info("Comprador %s (%s) cadastrado", club_user_id, nome)


def remove(club_user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM compradores WHERE club_user_id = ?", (int(club_user_id),)
        )
        conn.commit()
        return cur.rowcount > 0


def get(club_user_id: int) -> dict[str, Any] | None:
    init_table()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM compradores WHERE club_user_id = ?",
            (int(club_user_id),),
        ).fetchone()
        return dict(row) if row else None


def listar() -> list[dict[str, Any]]:
    init_table()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM compradores ORDER BY nome"
        ).fetchall()
        return [dict(r) for r in rows]


def resolve(club_user_id: int | None) -> tuple[str | None, str | None]:
    """Resolve (club_user_id) → (nome, email).

    Retorna (None, None) se o ID não está na tabela e não há fallback.
    """
    if not club_user_id:
        return None, None
    reg = get(club_user_id)
    if reg and reg.get("ativo"):
        return reg["nome"], reg["email"]
    return None, None
