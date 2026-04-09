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

-- Ações que o sistema TERIA tomado no Pipefy (ou tomou de fato).
-- Em modo "consulta", executada=0 e nenhuma chamada é feita ao Pipefy;
-- a linha serve como audit log preventivo. Em modo "automatico",
-- executada=1 após sucesso da mutation. Base para a futura UI de
-- reversão de ações automáticas.
CREATE TABLE IF NOT EXISTS acoes_pipefy_planejadas (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    validacao_id  INTEGER NOT NULL,
    oc_numero     TEXT    NOT NULL,
    card_id       TEXT,
    acao          TEXT    NOT NULL,  -- move_card | update_field | send_email | create_comment
    payload       TEXT    NOT NULL,  -- JSON do que seria/foi enviado
    motivo        TEXT,
    executada     INTEGER NOT NULL DEFAULT 0,
    erro          TEXT,
    criado_em     TEXT    NOT NULL,
    FOREIGN KEY (validacao_id) REFERENCES validacoes(id)
);

CREATE INDEX IF NOT EXISTS idx_acoes_validacao ON acoes_pipefy_planejadas(validacao_id);
CREATE INDEX IF NOT EXISTS idx_acoes_card ON acoes_pipefy_planejadas(card_id);

-- ----- Auth: perfis e usuários ----------------------------------------
-- Estrutura genérica de perfis para permitir criar mais perfis depois
-- pelo painel admin sem refator. Hoje só usamos "Admin".
CREATE TABLE IF NOT EXISTS perfis (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    nome        TEXT    UNIQUE NOT NULL,
    descricao   TEXT,
    permissoes  TEXT    NOT NULL DEFAULT '[]',  -- JSON array (futuro)
    criado_em   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS usuarios (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    username               TEXT    UNIQUE NOT NULL COLLATE NOCASE,
    nome                   TEXT    NOT NULL,
    email                  TEXT,
    senha_hash             TEXT    NOT NULL,
    perfil_id              INTEGER NOT NULL,
    ativo                  INTEGER NOT NULL DEFAULT 1,
    must_change_password   INTEGER NOT NULL DEFAULT 1,
    criado_em              TEXT    NOT NULL,
    ultimo_login           TEXT,
    FOREIGN KEY (perfil_id) REFERENCES perfis(id)
);

CREATE INDEX IF NOT EXISTS idx_usuarios_perfil ON usuarios(perfil_id);

-- ----- R2 cross-time: histórico de produtos e cache de devoluções -----
-- Histórico de produtos das OCs do Club, indexado por placa e data.
-- Populado incrementalmente: a cada validação, o orchestrator garante
-- que os últimos `R2_JANELA_DIAS` estão presentes (faz backfill se
-- necessário, depois só adiciona o D-1 do dia atual). Permite a R2
-- detectar reincidência (mesma peça/placa nos últimos N dias) sem
-- precisar bater no Club a cada execução.
CREATE TABLE IF NOT EXISTS historico_produtos_oc (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    data_oc            TEXT    NOT NULL,   -- ISO YYYY-MM-DD (data do pedido)
    id_pedido          TEXT    NOT NULL,
    id_cotacao         TEXT,
    placa_normalizada  TEXT    NOT NULL,   -- sem hífen, para casar com Pipefy
    identificador      TEXT,                -- com hífen, para display
    chave_produto      TEXT    NOT NULL,   -- "ean:..." | "cod:..." | "desc:..."
    descricao          TEXT,
    fornecedor_id      TEXT,
    fornecedor_nome    TEXT,
    quantidade         REAL,
    card_pipefy_id     TEXT,                -- preenchido se a OC tinha card
    criado_em          TEXT    NOT NULL,
    UNIQUE(id_pedido, chave_produto)
);

CREATE INDEX IF NOT EXISTS idx_hist_placa_data
    ON historico_produtos_oc(placa_normalizada, data_oc);
CREATE INDEX IF NOT EXISTS idx_hist_data
    ON historico_produtos_oc(data_oc);

-- Snapshot dos cards "em aberto" do pipe Devolução de Peças.
-- Recriado a cada validação (TRUNCATE + INSERT). Indexado por placa
-- normalizada (sem hífen) que é o campo "Placa" do start form lá.
-- Uma placa pode ter mais de uma devolução em aberto — não usamos
-- PRIMARY KEY, gravamos múltiplas linhas e a R2 usa "EXISTS".
CREATE TABLE IF NOT EXISTS cache_devolucoes (
    placa_normalizada  TEXT    NOT NULL,
    card_id            TEXT    NOT NULL,
    n_oc               TEXT,
    peca_descricao     TEXT,
    fase_atual         TEXT,
    atualizado_em      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dev_placa
    ON cache_devolucoes(placa_normalizada);

-- Snapshot dos cards em fases de cancelamento do PIPE PRINCIPAL.
-- Recriado a cada validação. Inclui dois "tipos":
--   "informacoes_incorretas": fase 334019348 (em revisão, podem voltar)
--   "cancelado":              fase 337982176 (terminal, definitivos)
-- Indexado por placa normalizada (sem hífen).
CREATE TABLE IF NOT EXISTS cache_cancelamentos (
    placa_normalizada  TEXT    NOT NULL,
    card_id            TEXT    NOT NULL,
    tipo               TEXT    NOT NULL,   -- 'informacoes_incorretas' | 'cancelado'
    fase_atual         TEXT,
    descricao_pecas    TEXT,               -- campo "Descrição das Peças" do card
    codigo_oc          TEXT,               -- campo "Código da OC" do card
    atualizado_em      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_canc_placa
    ON cache_cancelamentos(placa_normalizada);

-- Cache local de orçamentos consultados no Cilia, indexado por placa.
-- Cada placa tem no máximo 1 entrada (o orçamento mais recente). O TTL
-- é checado em runtime via `cache_cilia_get(placa, ttl_seconds)`.
-- O payload é o JSON serializado de `OrcamentoCilia` para preservar
-- todos os campos sem precisar denormalizar.
CREATE TABLE IF NOT EXISTS cache_cilia_orcamentos (
    placa_normalizada  TEXT    PRIMARY KEY,
    encontrado         INTEGER NOT NULL,    -- 1 se a placa tem orçamento, 0 se não
    payload_json       TEXT    NOT NULL,    -- JSON de OrcamentoCilia
    atualizado_em      TEXT    NOT NULL
);
"""


# Migrações aditivas — rodadas após o SCHEMA base. Cada entrada é
# (tabela, coluna, tipo). Usamos PRAGMA table_info para checar se a coluna
# já existe antes de tentar adicionar — mantém idempotência e preserva
# dados históricos.
_MIGRATIONS: list[tuple[str, str, str]] = [
    ("validacoes", "aguardando_ml", "INTEGER NOT NULL DEFAULT 0"),
    ("validacoes", "ja_processadas", "INTEGER NOT NULL DEFAULT 0"),
    ("oc_resultados", "fase_pipefy_atual", "TEXT"),
    ("oc_resultados", "valor_card", "REAL"),
    ("cache_cancelamentos", "descricao_pecas", "TEXT"),
    ("cache_cancelamentos", "codigo_oc", "TEXT"),
]


def _aplicar_migracoes(conn: sqlite3.Connection) -> None:
    for tabela, coluna, tipo in _MIGRATIONS:
        existentes = {row[1] for row in conn.execute(f"PRAGMA table_info({tabela})")}
        if coluna not in existentes:
            conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")


# Índices que dependem de colunas adicionadas por migrations — criados
# APÓS _aplicar_migracoes() para evitar "no such column" em bancos antigos.
_POST_MIGRATION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_canc_codigo_oc ON cache_cancelamentos(codigo_oc)",
    "CREATE INDEX IF NOT EXISTS idx_dev_n_oc ON cache_devolucoes(n_oc)",
]


def init_db() -> None:
    """Cria tabelas se não existirem, aplica migrações e habilita WAL."""
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _aplicar_migracoes(conn)
        for idx_sql in _POST_MIGRATION_INDEXES:
            conn.execute(idx_sql)
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
                fornecedor, comprador, forma_pagamento, valor_card, valor_club,
                valor_pdf, valor_cilia, qtd_cotacoes, qtd_produtos, peca_duplicada,
                status, regras_falhadas, fase_pipefy, card_pipefy_id, fase_pipefy_atual)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                validacao_id,
                payload.get("id_pedido"),
                payload.get("id_cotacao"),
                payload.get("placa"),
                payload.get("placa_normalizada"),
                payload.get("fornecedor"),
                payload.get("comprador"),
                payload.get("forma_pagamento"),
                payload.get("valor_card"),
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


def registrar_acao_planejada(
    validacao_id: int,
    oc_numero: str,
    card_id: str | None,
    acao: str,
    payload: dict[str, Any],
    motivo: str | None,
    executada: bool,
    erro: str | None = None,
) -> int:
    """Registra uma ação que o sistema tomou (executada=True) ou
    teria tomado em modo consulta (executada=False) no Pipefy/e-mail.
    """
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO acoes_pipefy_planejadas
               (validacao_id, oc_numero, card_id, acao, payload, motivo,
                executada, erro, criado_em)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                validacao_id,
                oc_numero,
                card_id,
                acao,
                json.dumps(payload, ensure_ascii=False, default=str),
                motivo,
                1 if executada else 0,
                erro,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def listar_acoes_planejadas(
    validacao_id: int | None = None, limite: int = 200
) -> list[dict[str, Any]]:
    with get_conn() as conn:
        if validacao_id is not None:
            rows = conn.execute(
                "SELECT * FROM acoes_pipefy_planejadas WHERE validacao_id = ? "
                "ORDER BY id DESC LIMIT ?",
                (validacao_id, limite),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM acoes_pipefy_planejadas ORDER BY id DESC LIMIT ?",
                (limite,),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["payload"] = json.loads(d["payload"])
            except (json.JSONDecodeError, TypeError):
                pass
            out.append(d)
        return out


# ---------- Auth: perfis ----------

def criar_perfil(nome: str, descricao: str | None = None, permissoes: list[str] | None = None) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO perfis (nome, descricao, permissoes, criado_em) VALUES (?, ?, ?, ?)",
            (
                nome,
                descricao,
                json.dumps(permissoes or [], ensure_ascii=False),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_perfil_por_nome(nome: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM perfis WHERE nome = ?", (nome,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["permissoes"] = json.loads(d["permissoes"])
        except (json.JSONDecodeError, TypeError):
            d["permissoes"] = []
        return d


def get_perfil(perfil_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM perfis WHERE id = ?", (perfil_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["permissoes"] = json.loads(d["permissoes"])
        except (json.JSONDecodeError, TypeError):
            d["permissoes"] = []
        return d


def listar_perfis() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM perfis ORDER BY id").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["permissoes"] = json.loads(d["permissoes"])
            except (json.JSONDecodeError, TypeError):
                d["permissoes"] = []
            out.append(d)
        return out


def atualizar_perfil(
    perfil_id: int,
    *,
    nome: str | None = None,
    descricao: str | None = None,
    permissoes: list[str] | None = None,
) -> bool:
    sets, params = [], []
    if nome is not None:
        sets.append("nome = ?")
        params.append(nome)
    if descricao is not None:
        sets.append("descricao = ?")
        params.append(descricao)
    if permissoes is not None:
        sets.append("permissoes = ?")
        params.append(json.dumps(permissoes, ensure_ascii=False))
    if not sets:
        return False
    params.append(perfil_id)
    with get_conn() as conn:
        cur = conn.execute(f"UPDATE perfis SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        return cur.rowcount > 0


# ---------- Auth: usuários ----------

def criar_usuario(
    username: str,
    nome: str,
    senha_hash: str,
    perfil_id: int,
    *,
    email: str | None = None,
    must_change_password: bool = True,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO usuarios
               (username, nome, email, senha_hash, perfil_id, ativo,
                must_change_password, criado_em)
               VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
            (
                username,
                nome,
                email,
                senha_hash,
                perfil_id,
                1 if must_change_password else 0,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_usuario_por_username(username: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM usuarios WHERE username = ? COLLATE NOCASE",
            (username,),
        ).fetchone()
        return dict(row) if row else None


def get_usuario(usuario_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM usuarios WHERE id = ?", (usuario_id,)
        ).fetchone()
        return dict(row) if row else None


def listar_usuarios() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT u.*, p.nome AS perfil_nome FROM usuarios u "
            "JOIN perfis p ON p.id = u.perfil_id ORDER BY u.id"
        ).fetchall()
        return [dict(r) for r in rows]


def atualizar_usuario(
    usuario_id: int,
    *,
    nome: str | None = None,
    email: str | None = None,
    perfil_id: int | None = None,
    ativo: bool | None = None,
) -> bool:
    sets, params = [], []
    if nome is not None:
        sets.append("nome = ?")
        params.append(nome)
    if email is not None:
        sets.append("email = ?")
        params.append(email)
    if perfil_id is not None:
        sets.append("perfil_id = ?")
        params.append(perfil_id)
    if ativo is not None:
        sets.append("ativo = ?")
        params.append(1 if ativo else 0)
    if not sets:
        return False
    params.append(usuario_id)
    with get_conn() as conn:
        cur = conn.execute(f"UPDATE usuarios SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        return cur.rowcount > 0


def atualizar_senha_usuario(
    usuario_id: int, senha_hash: str, *, must_change_password: bool = False
) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE usuarios SET senha_hash = ?, must_change_password = ? WHERE id = ?",
            (senha_hash, 1 if must_change_password else 0, usuario_id),
        )
        conn.commit()
        return cur.rowcount > 0


def registrar_login(usuario_id: int) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE usuarios SET ultimo_login = ? WHERE id = ?",
            (datetime.now().isoformat(timespec="seconds"), usuario_id),
        )
        conn.commit()


# ---------- R2 cross-time: histórico de produtos ----------

def registrar_historico_produtos(
    linhas: list[dict[str, Any]]
) -> int:
    """Insere/atualiza várias linhas de `historico_produtos_oc` em batch.

    Idempotente via `INSERT OR IGNORE` no UNIQUE(id_pedido, chave_produto).
    Retorna o número de linhas efetivamente inseridas (descontando os
    ignores). Cada linha é um dict com as chaves do schema.
    """
    if not linhas:
        return 0
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.executemany(
            """INSERT OR IGNORE INTO historico_produtos_oc
               (data_oc, id_pedido, id_cotacao, placa_normalizada,
                identificador, chave_produto, descricao, fornecedor_id,
                fornecedor_nome, quantidade, card_pipefy_id, criado_em)
               VALUES (:data_oc, :id_pedido, :id_cotacao, :placa_normalizada,
                       :identificador, :chave_produto, :descricao,
                       :fornecedor_id, :fornecedor_nome, :quantidade,
                       :card_pipefy_id, :criado_em)""",
            [{**l, "criado_em": agora} for l in linhas],
        )
        conn.commit()
        return cur.rowcount or 0


def buscar_reincidencias(
    placa_normalizada: str,
    chave_produto: str,
    *,
    data_max: str,
    dias: int,
    ignorar_id_pedido: str | None = None,
) -> list[dict[str, Any]]:
    """Busca registros anteriores da mesma peça (placa+chave) numa janela
    de N dias antes (e incluindo) `data_max`. Exclui opcionalmente um
    id_pedido (para não contar a própria OC sendo validada)."""
    sql = """
        SELECT * FROM historico_produtos_oc
        WHERE placa_normalizada = ?
          AND chave_produto = ?
          AND data_oc >= date(?, ?)
          AND data_oc <= ?
        """
    params: list[Any] = [
        placa_normalizada,
        chave_produto,
        data_max,
        f"-{int(dias)} days",
        data_max,
    ]
    if ignorar_id_pedido:
        sql += " AND id_pedido != ?"
        params.append(ignorar_id_pedido)
    sql += " ORDER BY data_oc DESC, id_pedido DESC"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def buscar_todas_duplicidades_placa(
    placa_normalizada: str,
    *,
    data_max: str,
    dias: int,
) -> list[dict[str, Any]]:
    """Retorna TODAS as peças compradas 2+ vezes para a mesma placa
    na janela de `dias` dias. Agrupa por chave_produto e retorna
    uma lista com cada grupo (todas as ocorrências).

    Usado para mostrar ao analista o histórico completo de duplicidades
    da placa, mesmo peças que não estão na OC do dia atual."""
    sql = """
        SELECT chave_produto, descricao, COUNT(*) as total_ocorrencias,
               GROUP_CONCAT(id_pedido, '|') as ids_pedido,
               GROUP_CONCAT(data_oc, '|') as datas_oc,
               GROUP_CONCAT(fornecedor_nome, '|') as fornecedores,
               GROUP_CONCAT(card_pipefy_id, '|') as cards_pipefy
        FROM historico_produtos_oc
        WHERE placa_normalizada = ?
          AND data_oc >= date(?, ?)
          AND data_oc <= ?
        GROUP BY chave_produto
        HAVING COUNT(*) > 1
        ORDER BY total_ocorrencias DESC, chave_produto
    """
    params = [
        placa_normalizada,
        data_max,
        f"-{int(dias)} days",
        data_max,
    ]
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def dias_presentes_no_historico(
    data_inicio: str, data_fim: str
) -> set[str]:
    """Retorna o conjunto de datas (YYYY-MM-DD) já presentes em
    `historico_produtos_oc` entre as datas dadas (inclusive)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT data_oc FROM historico_produtos_oc "
            "WHERE data_oc BETWEEN ? AND ? ORDER BY data_oc",
            (data_inicio, data_fim),
        ).fetchall()
        return {r["data_oc"] for r in rows}


# ---------- R2 cross-time: cache de devoluções ----------

def atualizar_cache_devolucoes(linhas: list[dict[str, Any]]) -> int:
    """Substitui (TRUNCATE + INSERT) o cache local de devoluções abertas.
    `linhas` é uma lista de dicts com as chaves do schema. Retorna o
    número de linhas inseridas."""
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM cache_devolucoes")
        if linhas:
            conn.executemany(
                """INSERT INTO cache_devolucoes
                   (placa_normalizada, card_id, n_oc, peca_descricao,
                    fase_atual, atualizado_em)
                   VALUES (:placa_normalizada, :card_id, :n_oc,
                           :peca_descricao, :fase_atual, :atualizado_em)""",
                [{**l, "atualizado_em": agora} for l in linhas],
            )
        conn.commit()
        return len(linhas)


def get_devolucoes_por_placa(placa_normalizada: str) -> list[dict[str, Any]]:
    """Retorna todos os cards de devolução em aberto para a placa dada."""
    with get_conn() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM cache_devolucoes WHERE placa_normalizada = ?",
                (placa_normalizada,),
            ).fetchall()
        ]


def get_devolucoes_por_oc(n_oc: str) -> list[dict[str, Any]]:
    """Retorna cards de devolução cujo campo n_oc casa com o id_pedido dado.

    Usado pela R2 cross-time para verificar se a OC ANTERIOR (que gerou
    a reincidência) tem devolução aberta — correlação direta por número
    da OC, mais precisa que busca por placa."""
    if not n_oc:
        return []
    with get_conn() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM cache_devolucoes WHERE n_oc = ?",
                (n_oc.strip(),),
            ).fetchall()
        ]


# ---------- R2 cross-time: cache de cancelamentos (pipe principal) ----------

def atualizar_cache_cancelamentos(linhas: list[dict[str, Any]]) -> int:
    """Substitui (TRUNCATE + INSERT) o cache local de cards em fases de
    cancelamento do pipe principal. `linhas` é uma lista de dicts com as
    chaves do schema. Retorna o número de linhas inseridas."""
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("DELETE FROM cache_cancelamentos")
        if linhas:
            conn.executemany(
                """INSERT INTO cache_cancelamentos
                   (placa_normalizada, card_id, tipo, fase_atual,
                    descricao_pecas, codigo_oc, atualizado_em)
                   VALUES (:placa_normalizada, :card_id, :tipo, :fase_atual,
                           :descricao_pecas, :codigo_oc, :atualizado_em)""",
                [{**l, "atualizado_em": agora} for l in linhas],
            )
        conn.commit()
        return len(linhas)


def get_cancelamentos_por_placa(placa_normalizada: str) -> list[dict[str, Any]]:
    """Retorna todos os cards de cancelamento (qualquer tipo) para a placa."""
    with get_conn() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM cache_cancelamentos WHERE placa_normalizada = ?",
                (placa_normalizada,),
            ).fetchall()
        ]


def get_cancelamentos_por_oc(codigo_oc: str) -> list[dict[str, Any]]:
    """Retorna cards de cancelamento cujo codigo_oc casa com o dado."""
    if not codigo_oc:
        return []
    with get_conn() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM cache_cancelamentos WHERE codigo_oc = ?",
                (codigo_oc.strip(),),
            ).fetchall()
        ]


# ---------- Cache de orçamentos Cilia ----------

def cache_cilia_get(
    placa_normalizada: str, *, ttl_seconds: int
) -> dict[str, Any] | None:
    """Retorna o payload do cache se a entrada existe e é mais recente
    que `ttl_seconds`. Retorna None caso contrário (cache miss ou stale).

    O payload é o dict bruto do JSON serializado de `OrcamentoCilia`,
    incluindo a flag `encontrado` (que vem na estrutura do orçamento).
    A camada chamadora é responsável por reconstruir o objeto Pydantic.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM cache_cilia_orcamentos WHERE placa_normalizada = ?",
            (placa_normalizada,),
        ).fetchone()
    if not row:
        return None
    try:
        atualizado = datetime.fromisoformat(row["atualizado_em"])
    except (TypeError, ValueError):
        return None
    idade_s = (datetime.now() - atualizado).total_seconds()
    if idade_s > ttl_seconds:
        return None  # stale
    try:
        return json.loads(row["payload_json"])
    except (TypeError, json.JSONDecodeError):
        return None


def cache_cilia_set(
    placa_normalizada: str,
    *,
    encontrado: bool,
    payload: dict[str, Any],
) -> None:
    """Persiste (UPSERT) um orçamento no cache. `payload` deve ser o
    dict serializável correspondente a `OrcamentoCilia.model_dump()`."""
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO cache_cilia_orcamentos
               (placa_normalizada, encontrado, payload_json, atualizado_em)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(placa_normalizada) DO UPDATE SET
                   encontrado = excluded.encontrado,
                   payload_json = excluded.payload_json,
                   atualizado_em = excluded.atualizado_em""",
            (
                placa_normalizada,
                1 if encontrado else 0,
                json.dumps(payload, ensure_ascii=False, default=str),
                agora,
            ),
        )
        conn.commit()


def cache_cilia_invalidate(placa_normalizada: str) -> None:
    """Remove uma entrada específica do cache (para 'force refresh')."""
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM cache_cilia_orcamentos WHERE placa_normalizada = ?",
            (placa_normalizada,),
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
