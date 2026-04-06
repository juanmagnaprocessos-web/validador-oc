"""SQLite WAL para histórico e auditoria (Requisito 5.4 — rastreabilidade).

Usa sqlite3 raw, sem ORM, seguindo o padrão do projeto gestao-pop.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from app.config import settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS validacoes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    data_execucao   TEXT    NOT NULL,
    data_d1         TEXT    NOT NULL,
    total_ocs       INTEGER NOT NULL DEFAULT 0,
    aprovadas       INTEGER NOT NULL DEFAULT 0,
    divergentes     INTEGER NOT NULL DEFAULT 0,
    bloqueadas      INTEGER NOT NULL DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'pendente_revisao',
    dry_run         INTEGER NOT NULL DEFAULT 1,
    relatorio_html  TEXT,
    relatorio_xlsx  TEXT,
    executado_por   TEXT
);

CREATE INDEX IF NOT EXISTS idx_validacoes_data_d1 ON validacoes(data_d1);

CREATE TABLE IF NOT EXISTS oc_resultados (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    validacao_id    INTEGER NOT NULL,
    id_pedido       TEXT    NOT NULL,
    id_cotacao      TEXT,
    placa           TEXT,
    placa_normalizada TEXT,
    fornecedor      TEXT,
    comprador       TEXT,
    forma_pagamento TEXT,
    valor_club      REAL,
    valor_pdf       REAL,
    valor_cilia     REAL,
    qtd_cotacoes    INTEGER,
    qtd_produtos    INTEGER,
    peca_duplicada  TEXT,
    status          TEXT NOT NULL,
    regras_falhadas TEXT,
    fase_pipefy     TEXT,
    card_pipefy_id  TEXT,
    FOREIGN KEY (validacao_id) REFERENCES validacoes(id)
);

CREATE INDEX IF NOT EXISTS idx_oc_resultados_validacao ON oc_resultados(validacao_id);
CREATE INDEX IF NOT EXISTS idx_oc_resultados_placa ON oc_resultados(placa_normalizada);

CREATE TABLE IF NOT EXISTS auditoria_api (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    TEXT NOT NULL,
    sistema      TEXT NOT NULL,  -- club | pipefy | cilia
    metodo       TEXT NOT NULL,
    url          TEXT NOT NULL,
    status_code  INTEGER,
    duracao_ms   INTEGER,
    erro         TEXT
);

CREATE INDEX IF NOT EXISTS idx_auditoria_timestamp ON auditoria_api(timestamp);
"""


# Migrações aditivas — rodadas após o SCHEMA base. Cada entrada é
# (tabela, coluna, tipo). Usamos PRAGMA table_info para checar se a coluna
# já existe antes de tentar adicionar — mantém idempotência e preserva
# dados históricos.
_MIGRATIONS: list[tuple[str, str, str]] = [
    ("validacoes", "aguardando_ml", "INTEGER NOT NULL DEFAULT 0"),
    ("validacoes", "ja_processadas", "INTEGER NOT NULL DEFAULT 0"),
    ("oc_resultados", "fase_pipefy_atual", "TEXT"),
]


def _aplicar_migracoes(conn: sqlite3.Connection) -> None:
    for tabela, coluna, tipo in _MIGRATIONS:
        existentes = {row[1] for row in conn.execute(f"PRAGMA table_info({tabela})")}
        if coluna not in existentes:
            conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")


def init_db() -> None:
    """Cria tabelas se não existirem, aplica migrações e habilita WAL."""
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _aplicar_migracoes(conn)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """Context manager de conexão SQLite com row_factory dict-like."""
    conn = sqlite3.connect(settings.db_full_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
    finally:
        conn.close()


# ---------- Persistência de validações ----------

def registrar_validacao(
    data_d1: str,
    total_ocs: int,
    aprovadas: int,
    divergentes: int,
    bloqueadas: int,
    dry_run: bool,
    executado_por: str,
    relatorio_html: str | None = None,
    relatorio_xlsx: str | None = None,
    aguardando_ml: int = 0,
    ja_processadas: int = 0,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO validacoes
               (data_execucao, data_d1, total_ocs, aprovadas, divergentes, bloqueadas,
                dry_run, relatorio_html, relatorio_xlsx, executado_por,
                aguardando_ml, ja_processadas)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(timespec="seconds"),
                data_d1,
                total_ocs,
                aprovadas,
                divergentes,
                bloqueadas,
                1 if dry_run else 0,
                relatorio_html,
                relatorio_xlsx,
                executado_por,
                aguardando_ml,
                ja_processadas,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def registrar_oc_resultado(validacao_id: int, payload: dict[str, Any]) -> None:
    regras = payload.get("regras_falhadas")
    if isinstance(regras, (list, dict)):
        regras = json.dumps(regras, ensure_ascii=False)

    with get_conn() as conn:
        conn.execute(
            """INSERT INTO oc_resultados
               (validacao_id, id_pedido, id_cotacao, placa, placa_normalizada,
                fornecedor, comprador, forma_pagamento, valor_club, valor_pdf,
                valor_cilia, qtd_cotacoes, qtd_produtos, peca_duplicada, status,
                regras_falhadas, fase_pipefy, card_pipefy_id, fase_pipefy_atual)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                validacao_id,
                payload.get("id_pedido"),
                payload.get("id_cotacao"),
                payload.get("placa"),
                payload.get("placa_normalizada"),
                payload.get("fornecedor"),
                payload.get("comprador"),
                payload.get("forma_pagamento"),
                payload.get("valor_club"),
                payload.get("valor_pdf"),
                payload.get("valor_cilia"),
                payload.get("qtd_cotacoes"),
                payload.get("qtd_produtos"),
                payload.get("peca_duplicada"),
                payload.get("status"),
                regras,
                payload.get("fase_pipefy"),
                payload.get("card_pipefy_id"),
                payload.get("fase_pipefy_atual"),
            ),
        )
        conn.commit()


def registrar_chamada_api(
    sistema: str,
    metodo: str,
    url: str,
    status_code: int | None,
    duracao_ms: int,
    erro: str | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO auditoria_api
               (timestamp, sistema, metodo, url, status_code, duracao_ms, erro)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(timespec="seconds"),
                sistema,
                metodo,
                url,
                status_code,
                duracao_ms,
                erro,
            ),
        )
        conn.commit()


def listar_historico(limite: int = 30) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM validacoes ORDER BY data_execucao DESC LIMIT ?",
            (limite,),
        ).fetchall()
        return [dict(r) for r in rows]


def resultados_de(validacao_id: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM oc_resultados WHERE validacao_id = ? ORDER BY id",
            (validacao_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("regras_falhadas"):
                try:
                    d["regras_falhadas"] = json.loads(d["regras_falhadas"])
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append(d)
        return result
