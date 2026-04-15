"""Camada de compatibilidade dual SQLite / Postgres (Neon).

Expoe `get_conn()` que retorna um ConnectionWrapper com a mesma API
que `sqlite3.Connection` (execute, executescript, commit, close, cursor
com lastrowid e fetch/iter). Internamente delega para sqlite3 ou psycopg
conforme `settings.db_dialect`.

Pontos de adaptacao:
  - placeholders: SQLite usa `?`, Postgres `%s`. O wrapper faz a troca
    em runtime.
  - `INSERT` + `lastrowid`: psycopg nao tem `lastrowid`. Qualquer INSERT
    que terminar com a palavra reservada `RETURNING id` (maiusculas) ganha
    suporte transparente a `cur.lastrowid` via fetchone().
  - `executescript` nao existe em psycopg. Splitamos por `;` ignorando
    blocos aninhados (schema nao usa).
  - Row factory dict-like: sqlite3.Row ou psycopg.rows.dict_row.
  - PRAGMA so eh emitido em SQLite (no-op em Postgres).

Nao cobre: uppercase SQL, strings com `?` literal (nao ha no codigo).
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Iterator

from app.config import settings


# SQLite nao tem suporte nativo a Decimal; registra um adapter que converte
# para float (suficiente para os valores monetarios do projeto que ja sao
# arredondados a 2 casas). Postgres via psycopg aceita Decimal nativamente.
sqlite3.register_adapter(Decimal, float)


def _adapt_placeholders(sql: str, dialect: str) -> str:
    """Troca `?` por `%s` no Postgres. SQLite mantem."""
    if dialect != "postgres":
        return sql
    # Simples: o codigo nao usa `?` literal em strings. Troca direta OK.
    return sql.replace("?", "%s")


class _CursorWrapper:
    """Cursor com API parecida com sqlite3.Cursor."""

    def __init__(self, cur: Any, dialect: str) -> None:
        self._cur = cur
        self._dialect = dialect
        # Para Postgres com RETURNING id, capturamos o id na execucao
        self._lastrowid: int | None = None

    def fetchone(self) -> Any:
        return self._cur.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)

    @property
    def lastrowid(self) -> int | None:
        if self._dialect == "postgres":
            return self._lastrowid
        return self._cur.lastrowid

    @property
    def rowcount(self) -> int:
        # psycopg e sqlite3 ambos expoem rowcount no cursor. Retorna -1 se
        # indisponivel (evita AttributeError em testes antigos).
        rc = getattr(self._cur, "rowcount", -1)
        return rc if rc is not None else -1


class _ConnectionWrapper:
    """Connection com API compativel com sqlite3.Connection."""

    def __init__(self, conn: Any, dialect: str) -> None:
        self._conn = conn
        self.dialect = dialect

    def execute(self, sql: str, params: tuple | list | dict = ()) -> _CursorWrapper:
        sql_a = _adapt_placeholders(sql, self.dialect)
        has_returning = "RETURNING" in sql_a.upper()
        if self.dialect == "postgres":
            cur = self._conn.cursor()
            cur.execute(sql_a, params or None)
            w = _CursorWrapper(cur, self.dialect)
        else:
            raw_cur = self._conn.execute(sql_a, params)
            w = _CursorWrapper(raw_cur, self.dialect)
        # Capturar RETURNING id logo apos execucao (em ambos dialetos)
        # para que o caller possa chamar commit() sem segurar cursor aberto.
        # Se `ON CONFLICT DO NOTHING` + `RETURNING id` e ocorreu conflito,
        # fetchone() retorna None — mantemos _lastrowid=None e o caller
        # deve tratar (ex: nao fazer int(cur.lastrowid) sem checar).
        if has_returning:
            try:
                row = w._cur.fetchone()
            except Exception:
                row = None
            if row is not None:
                if isinstance(row, dict):
                    # Prioriza chave 'id' para evitar depender da ordem de
                    # dict quando RETURNING tem multiplos campos.
                    if "id" in row:
                        w._lastrowid = int(row["id"])
                    else:
                        w._lastrowid = int(next(iter(row.values())))
                else:
                    w._lastrowid = int(row[0])
        return w

    def executescript(self, script: str) -> None:
        if self.dialect == "postgres":
            # Postgres nao tem executescript. Divide por `;` e executa cada.
            # Comentarios de linha unica (-- ...) sao suportados pelo server.
            for stmt in script.split(";"):
                s = stmt.strip()
                if s:
                    self._conn.cursor().execute(s)
        else:
            self._conn.executescript(script)

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def cursor(self) -> Any:
        """Expoe cursor raw (uso avancado)."""
        return self._conn.cursor()


def _connect_sqlite() -> _ConnectionWrapper:
    db_path = settings.db_full_path
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return _ConnectionWrapper(conn, "sqlite")


def _connect_postgres() -> _ConnectionWrapper:
    import psycopg
    from psycopg.rows import dict_row

    # Remove whitespace interno (espaços, quebras de linha, tabs) que pode
    # entrar quando DATABASE_URL é colada com wrap no painel do Render/Neon.
    # psycopg explode com mensagens enigmáticas (ex: "invalid sslmode value:
    # 'r  equire'") quando o valor chega quebrado. Sanitizar é defensivo e
    # inofensivo: URLs Postgres válidas não contêm whitespace.
    url = "".join(settings.database_url.split())
    # Neon aceita channel_binding=require mas psycopg pode nao suportar
    # diretamente — removemos se presente (sslmode=require ja basta).
    if "channel_binding=" in url:
        url = "&".join(
            p for p in url.replace("?", "&", 1).split("&")
            if not p.startswith("channel_binding=")
        ).replace("&", "?", 1)
    conn = psycopg.connect(url, row_factory=dict_row, connect_timeout=30)
    return _ConnectionWrapper(conn, "postgres")


@contextmanager
def get_conn() -> Iterator[_ConnectionWrapper]:
    """Context manager de conexao com o banco (SQLite ou Postgres).

    Decide pelo `settings.db_dialect`:
      - "postgres": usa psycopg + Neon/DATABASE_URL
      - "sqlite":   usa sqlite3 local
    """
    if settings.db_dialect == "postgres":
        w = _connect_postgres()
    else:
        w = _connect_sqlite()
    try:
        yield w
    finally:
        w.close()
