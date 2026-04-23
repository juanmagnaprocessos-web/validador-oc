"""Persistencia dual SQLite (dev) / Postgres Neon (prod).

Usa sqlite3 raw ou psycopg conforme `settings.db_dialect`, abstraidos
via `app._dbconn.get_conn`. Funcoes de negocio (registrar_validacao,
listar_historico, etc.) sao agnosticas ao dialeto.
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from app._dbconn import get_conn
from app.config import settings

logger = logging.getLogger(__name__)


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

-- Marca dias que ja foram consultados no Club (mesmo sem dados).
-- Evita re-consultar dias vazios (fins de semana, feriados) a cada backfill.
CREATE TABLE IF NOT EXISTS historico_dias_processados (
    data_oc      TEXT    PRIMARY KEY,
    tinha_dados  INTEGER NOT NULL DEFAULT 0,
    processado_em TEXT   NOT NULL
);

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

-- Lock persistente do CRON diário. PK = data_d1 garante que cada dia
-- só roda uma vez; TTL em expires_at permite que um processo que crashou
-- segurando o lock seja desalojado por outro após 2h (CRON_LOCK_TTL_S).
-- status: 'rodando' | 'sucesso' | 'vazio' | 'falha'. Só 'sucesso' e 'vazio'
-- impedem re-aquisição dentro do TTL; 'rodando' expirado libera; 'falha'
-- permite retry imediato.
CREATE TABLE IF NOT EXISTS cron_locks (
    data_d1       TEXT    PRIMARY KEY,
    acquired_at   TEXT    NOT NULL,
    expires_at    TEXT    NOT NULL,
    host          TEXT    NOT NULL,
    status        TEXT    NOT NULL,
    tentativa     INTEGER NOT NULL DEFAULT 1,
    last_error    TEXT,
    updated_at    TEXT    NOT NULL
);

-- Log de tentativas de autenticacao (sucesso e falha). Serve a dois
-- propositos: (1) auditoria forense, (2) storage persistente do rate
-- limiter — contador vem de COUNT(*) WHERE ip=? AND ts > ? AND
-- resultado != 'sucesso'. Persistencia em SQL garante que reinicio
-- do app (hibernacao no Render Free) nao zere o contador.
--
-- resultado: 'sucesso' | 'senha_errada' | 'usuario_inexistente' |
--            'usuario_desativado' | 'rate_limited_ip' |
--            'rate_limited_usuario' | 'credenciais_ausentes'
CREATE TABLE IF NOT EXISTS login_attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL,            -- ISO-8601 UTC
    ip          TEXT    NOT NULL,            -- IPv4 ou IPv6 ja normalizado (/64 em v6)
    username    TEXT    NOT NULL,            -- username tentado (mesmo se nao existir)
    user_agent  TEXT,                         -- truncado em 500 chars
    resultado   TEXT    NOT NULL,
    rota        TEXT                          -- path do endpoint atacado (opcional)
);

CREATE INDEX IF NOT EXISTS idx_login_attempts_ts
    ON login_attempts(ts);
CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_ts
    ON login_attempts(ip, ts);
CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_user_ts
    ON login_attempts(ip, username, ts);
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
    # --- Sessão 11: dados enriquecidos para tela de revisão ---
    ("oc_resultados", "divergencias_json", "TEXT"),       # JSON completo das divergências (com dados/links)
    ("oc_resultados", "produtos_json", "TEXT"),            # JSON da lista de produtos da OC
    ("oc_resultados", "reincidencia", "TEXT DEFAULT '—'"), # resumo reincidência
    ("oc_resultados", "cancelamento", "TEXT DEFAULT '—'"), # resumo cancelamento
    ("oc_resultados", "cancelamento_card_id", "TEXT"),
    ("oc_resultados", "card_pipefy_link", "TEXT"),
    ("oc_resultados", "forma_pagamento_canonica", "TEXT"),
    # --- Sessão 13: CRON diário ---
    ("validacoes", "origem", "TEXT NOT NULL DEFAULT 'manual'"),
]


def _aplicar_migracoes(conn: Any) -> None:
    """Aplica migracoes aditivas (adicionar colunas novas) de forma
    idempotente. Checa existencia via information_schema (Postgres) ou
    PRAGMA table_info (SQLite).
    """
    for tabela, coluna, tipo in _MIGRATIONS:
        if conn.dialect == "postgres":
            # information_schema é case-sensitive e minúsculas em Postgres.
            existe = conn.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = ? AND column_name = ?",
                (tabela, coluna),
            ).fetchone()
            if existe:
                continue
            tipo_pg = _sqlite_type_to_postgres(tipo)
            conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo_pg}")
        else:
            existentes = {row[1] for row in conn.execute(f"PRAGMA table_info({tabela})")}
            if coluna not in existentes:
                conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")


def _sqlite_type_to_postgres(tipo: str) -> str:
    """Mapeia tipos SQLite -> Postgres (suficiente para as migracoes atuais)."""
    t = tipo.strip()
    low = t.upper()
    if low.startswith("INTEGER"):
        return t.replace("INTEGER", "INTEGER", 1)  # Postgres aceita INTEGER
    if low.startswith("TEXT"):
        return t.replace("TEXT", "TEXT", 1)
    if low.startswith("REAL"):
        return t.replace("REAL", "DOUBLE PRECISION", 1)
    return t


# Índices que dependem de colunas adicionadas por migrations — criados
# APÓS _aplicar_migracoes() para evitar "no such column" em bancos antigos.
_POST_MIGRATION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_canc_codigo_oc ON cache_cancelamentos(codigo_oc)",
    "CREATE INDEX IF NOT EXISTS idx_dev_n_oc ON cache_devolucoes(n_oc)",
]


def _seed_usuarios(conn) -> None:
    """Cria perfil Admin e usuarios padrao se o banco estiver vazio."""
    row = conn.execute("SELECT COUNT(*) AS n FROM usuarios").fetchone()
    if row["n"] > 0:
        return

    from app.services.auth import hash_senha

    agora = datetime.now().isoformat(timespec="seconds")
    logger.info("Seed: criando perfil Admin e usuarios padrao")

    # Criar perfil Admin se nao existir
    perfil_row = conn.execute("SELECT id FROM perfis WHERE nome='Admin'").fetchone()
    if not perfil_row:
        conn.execute(
            "INSERT INTO perfis (nome, descricao, permissoes, criado_em) VALUES (?, ?, ?, ?)",
            ("Admin", "Administrador com acesso total", '["*"]', agora),
        )

    perfil_id = conn.execute("SELECT id FROM perfis WHERE nome='Admin'").fetchone()["id"]

    for username, nome, senha in [
        ("admin", "Administrador", "admin123"),
        ("juanpablo", "Juan Pablo", "admin123"),
    ]:
        conn.execute(
            """INSERT INTO usuarios
               (username, nome, email, senha_hash, perfil_id, ativo,
                must_change_password, criado_em)
               VALUES (?, ?, NULL, ?, ?, 1, 0, ?)""",
            (username, nome, hash_senha(senha), perfil_id, agora),
        )


def init_db() -> None:
    """Cria tabelas se nao existirem, aplica migracoes e, em SQLite, ativa WAL."""
    with get_conn() as conn:
        if conn.dialect == "postgres":
            # Carrega schema Postgres (BIGSERIAL, LOWER() em vez de COLLATE).
            schema_pg_path = Path(__file__).resolve().parent / "schema_postgres.sql"
            schema_pg = schema_pg_path.read_text(encoding="utf-8")
            conn.executescript(schema_pg)
        else:
            conn.executescript(SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=FULL;")
            conn.execute("PRAGMA wal_autocheckpoint=1000;")
        _aplicar_migracoes(conn)
        for idx_sql in _POST_MIGRATION_INDEXES:
            conn.execute(idx_sql)
        _seed_usuarios(conn)
        conn.commit()


def backup_db() -> Path:
    """Backup so faz sentido para SQLite local. Em Postgres o Neon
    gerencia snapshots/point-in-time-recovery pelo painel.
    """
    if settings.db_dialect == "postgres":
        logger.info("Postgres: backup gerenciado pelo provedor (Neon)")
        return Path("(postgres-managed)")

    db_path = Path(settings.db_full_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Banco nao encontrado: {db_path}")

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    finally:
        conn.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_suffix(f".db.bak.{timestamp}")
    shutil.copy2(str(db_path), str(backup_path))
    logger.info("Backup criado: %s", backup_path)

    padrao = f"{db_path.stem}.db.bak.*"
    backups = sorted(
        db_path.parent.glob(padrao),
        key=lambda p: p.stat().st_mtime,
    )
    while len(backups) > 5:
        antigo = backups.pop(0)
        antigo.unlink()
        logger.info("Backup antigo removido: %s", antigo)

    return backup_path


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
    origem: str = "manual",
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO validacoes
               (data_execucao, data_d1, total_ocs, aprovadas, divergentes, bloqueadas,
                dry_run, relatorio_html, relatorio_xlsx, executado_por,
                aguardando_ml, ja_processadas, origem)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""",
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
                origem,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def registrar_oc_resultado(validacao_id: int, payload: dict[str, Any]) -> None:
    regras = payload.get("regras_falhadas")
    if isinstance(regras, (list, dict)):
        regras = json.dumps(regras, ensure_ascii=False)

    divergencias_json = payload.get("divergencias_json")
    if isinstance(divergencias_json, (list, dict)):
        divergencias_json = json.dumps(divergencias_json, ensure_ascii=False, default=str)

    produtos_json = payload.get("produtos_json")
    if isinstance(produtos_json, (list, dict)):
        produtos_json = json.dumps(produtos_json, ensure_ascii=False, default=str)

    with get_conn() as conn:
        conn.execute(
            """INSERT INTO oc_resultados
               (validacao_id, id_pedido, id_cotacao, placa, placa_normalizada,
                fornecedor, comprador, forma_pagamento, valor_card, valor_club,
                valor_pdf, valor_cilia, qtd_cotacoes, qtd_produtos, peca_duplicada,
                status, regras_falhadas, fase_pipefy, card_pipefy_id, fase_pipefy_atual,
                divergencias_json, produtos_json, reincidencia, cancelamento,
                cancelamento_card_id, card_pipefy_link, forma_pagamento_canonica)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?)""",
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
                divergencias_json,
                produtos_json,
                payload.get("reincidencia", "—"),
                payload.get("cancelamento", "—"),
                payload.get("cancelamento_card_id"),
                payload.get("card_pipefy_link"),
                payload.get("forma_pagamento_canonica"),
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
    from app.utils.sanitize import sanitizar_url

    url_limpa = sanitizar_url(url)
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO auditoria_api
               (timestamp, sistema, metodo, url, status_code, duracao_ms, erro)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(timespec="seconds"),
                sistema,
                metodo,
                url_limpa,
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
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""",
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
        # Se perfil ja existe, retorna o id existente
        existing = conn.execute("SELECT id FROM perfis WHERE nome = ?", (nome,)).fetchone()
        if existing:
            return int(existing["id"])
        cur = conn.execute(
            "INSERT INTO perfis (nome, descricao, permissoes, criado_em) VALUES (?, ?, ?, ?) RETURNING id",
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
               VALUES (?, ?, ?, ?, ?, 1, ?, ?) RETURNING id""",
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
    # LOWER() funciona em SQLite e Postgres; remove a dependencia de
    # COLLATE NOCASE (SQLite-only).
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM usuarios WHERE LOWER(username) = LOWER(?)",
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


# ---------- login_attempts: log + rate limiter backend ----------

def registrar_tentativa_login(
    *,
    ts: str,
    ip: str,
    username: str,
    user_agent: str | None,
    resultado: str,
    rota: str | None = None,
) -> None:
    """Insere uma linha em login_attempts. `ts` deve vir pronto em ISO-8601
    (UTC) — o caller controla pra permitir mock de tempo nos testes."""
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO login_attempts
               (ts, ip, username, user_agent, resultado, rota)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ts, ip, username or "", user_agent, resultado, rota),
        )
        conn.commit()


def contar_falhas_recentes(
    *,
    ip: str,
    username: str | None,
    desde_iso: str,
) -> int:
    """Conta tentativas NAO-sucesso desde `desde_iso` (exclusivo). Se
    username fornecido, chave = (ip, username). Caso contrario, so (ip).

    Observacao: conta qualquer resultado que nao seja 'sucesso' — inclui
    tentativas previamente rate_limited pra evitar 'piscar' (atacante que
    atingiu o teto e espera um pouco teria o contador reinserindo essas
    linhas)."""
    sql = (
        "SELECT COUNT(*) AS n FROM login_attempts "
        "WHERE ip = ? AND ts > ? AND resultado != 'sucesso'"
    )
    params: list[Any] = [ip, desde_iso]
    if username is not None:
        sql += " AND LOWER(username) = LOWER(?)"
        params.append(username)
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
    return int(row["n"] if row else 0)


def listar_tentativas_login(
    *,
    limite: int = 100,
    ip: str | None = None,
    username: str | None = None,
    resultado: str | None = None,
) -> list[dict[str, Any]]:
    """Lista tentativas mais recentes pra endpoint admin."""
    filtros = []
    params: list[Any] = []
    if ip:
        filtros.append("ip = ?")
        params.append(ip)
    if username:
        filtros.append("LOWER(username) = LOWER(?)")
        params.append(username)
    if resultado:
        filtros.append("resultado = ?")
        params.append(resultado)
    where = (" WHERE " + " AND ".join(filtros)) if filtros else ""
    limite = max(1, min(500, int(limite)))
    sql = (
        f"SELECT id, ts, ip, username, user_agent, resultado, rota "
        f"FROM login_attempts{where} ORDER BY ts DESC LIMIT {limite}"
    )
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def unlock_tentativas_login(
    *,
    username: str | None = None,
    ip: str | None = None,
    janela_horas: int = 24,
) -> int:
    """Deleta tentativas NAO-sucesso dentro da janela (default 24h) que
    correspondam aos filtros. Usado pelo CLI de emergencia quando um admin
    fica preso. Pelo menos um filtro (username OU ip) e obrigatorio.

    IMPORTANTE: so apaga dentro da `janela_horas` — preserva historico
    forense mais antigo. Se atacante persistir por semanas, o log completo
    continua disponivel mesmo apos um unlock.
    """
    if not username and not ip:
        return 0
    corte = datetime.now().isoformat(timespec="seconds")
    # Usamos datetime do Python e comparamos como string ISO (alinhado
    # com o padrao de timestamps do projeto).
    from datetime import timedelta as _td
    desde = (
        datetime.now() - _td(hours=max(1, int(janela_horas)))
    ).isoformat(timespec="seconds")

    filtros = ["resultado != 'sucesso'", "ts > ?"]
    params: list[Any] = [desde]
    if username:
        filtros.append("LOWER(username) = LOWER(?)")
        params.append(username)
    if ip:
        filtros.append("ip = ?")
        params.append(ip)
    sql = f"DELETE FROM login_attempts WHERE {' AND '.join(filtros)}"
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


def purgar_tentativas_login_antigas(*, ate_iso: str) -> int:
    """Deleta tentativas com ts <= ate_iso. Chamado pelo job de retention
    (90d padrao). Retorna quantas linhas removeu."""
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM login_attempts WHERE ts <= ?",
            (ate_iso,),
        )
        conn.commit()
        return cur.rowcount


# ---------- R2 cross-time: histórico de produtos ----------

def registrar_historico_produtos(
    linhas: list[dict[str, Any]]
) -> int:
    """Insere varias linhas de `historico_produtos_oc` em batch.

    Idempotente via `ON CONFLICT DO NOTHING` no UNIQUE(id_pedido, chave_produto).
    `ON CONFLICT DO NOTHING` e suportado em SQLite >=3.24 e em Postgres.
    Retorna o numero de linhas inseridas. Cada linha e um dict com as
    chaves do schema.
    """
    if not linhas:
        return 0
    agora = datetime.now().isoformat(timespec="seconds")
    inseridos = 0
    with get_conn() as conn:
        for l in linhas:
            cur = conn.execute(
                """INSERT INTO historico_produtos_oc
                   (data_oc, id_pedido, id_cotacao, placa_normalizada,
                    identificador, chave_produto, descricao, fornecedor_id,
                    fornecedor_nome, quantidade, card_pipefy_id, criado_em)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (id_pedido, chave_produto) DO NOTHING""",
                (
                    l.get("data_oc"),
                    l.get("id_pedido"),
                    l.get("id_cotacao"),
                    l.get("placa_normalizada"),
                    l.get("identificador"),
                    l.get("chave_produto"),
                    l.get("descricao"),
                    l.get("fornecedor_id"),
                    l.get("fornecedor_nome"),
                    l.get("quantidade"),
                    l.get("card_pipefy_id"),
                    agora,
                ),
            )
            # rowcount indica se inseriu (1) ou ignorou (0)
            if getattr(cur._cur, "rowcount", 0) > 0:
                inseridos += 1
        conn.commit()
    return inseridos


def _data_min_janela(data_max: str, dias: int) -> str:
    """Calcula ISO date de `data_max - dias`, feito em Python para ser
    agnostico ao dialeto SQL (SQLite usa date(?, '-N days'), Postgres
    nao tem esse shorthand)."""
    from datetime import date as _date, timedelta
    base = _date.fromisoformat(data_max)
    return (base - timedelta(days=int(dias))).isoformat()


def buscar_reincidencias(
    placa_normalizada: str,
    chave_produto: str,
    *,
    data_max: str,
    dias: int,
    ignorar_id_pedido: str | None = None,
) -> list[dict[str, Any]]:
    """Busca registros anteriores da mesma peca (placa+chave) numa janela
    de N dias antes (e incluindo) `data_max`. Exclui opcionalmente um
    id_pedido (para nao contar a propria OC sendo validada)."""
    data_min = _data_min_janela(data_max, dias)
    sql = """
        SELECT * FROM historico_produtos_oc
        WHERE placa_normalizada = ?
          AND chave_produto = ?
          AND data_oc >= ?
          AND data_oc <= ?
        """
    params: list[Any] = [
        placa_normalizada,
        chave_produto,
        data_min,
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
    """Retorna TODAS as pecas compradas 2+ vezes para a mesma placa
    na janela de `dias` dias. Agrupa por chave_produto e retorna
    uma lista com cada grupo (todas as ocorrencias).

    Usa STRING_AGG (Postgres) ou GROUP_CONCAT (SQLite) conforme o dialeto
    ativo, ja que nao ha agregador de strings ANSI-SQL portavel."""
    data_min = _data_min_janela(data_max, dias)
    if settings.db_dialect == "postgres":
        agg = "STRING_AGG"
        # STRING_AGG exige texto — garantimos cast explicito em card_pipefy_id
        # (pode ser TEXT ou NULL; STRING_AGG ignora NULLs automaticamente)
        sql = """
            SELECT chave_produto, descricao, COUNT(*) as total_ocorrencias,
                   STRING_AGG(id_pedido, '|') as ids_pedido,
                   STRING_AGG(data_oc, '|') as datas_oc,
                   STRING_AGG(fornecedor_nome, '|') as fornecedores,
                   STRING_AGG(card_pipefy_id, '|') as cards_pipefy
            FROM historico_produtos_oc
            WHERE placa_normalizada = ?
              AND data_oc >= ?
              AND data_oc <= ?
            GROUP BY chave_produto, descricao
            HAVING COUNT(*) > 1
            ORDER BY total_ocorrencias DESC, chave_produto
        """
    else:
        sql = """
            SELECT chave_produto, descricao, COUNT(*) as total_ocorrencias,
                   GROUP_CONCAT(id_pedido, '|') as ids_pedido,
                   GROUP_CONCAT(data_oc, '|') as datas_oc,
                   GROUP_CONCAT(fornecedor_nome, '|') as fornecedores,
                   GROUP_CONCAT(card_pipefy_id, '|') as cards_pipefy
            FROM historico_produtos_oc
            WHERE placa_normalizada = ?
              AND data_oc >= ?
              AND data_oc <= ?
            GROUP BY chave_produto
            HAVING COUNT(*) > 1
            ORDER BY total_ocorrencias DESC, chave_produto
        """
    params = [placa_normalizada, data_min, data_max]
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


def dias_ja_processados(data_inicio: str, data_fim: str) -> set[str]:
    """Retorna o conjunto de datas ja processadas pelo backfill (mesmo
    que tenham retornado 0 OCs). Usado para evitar re-consultar dias
    vazios a cada backfill (fins de semana, feriados)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT data_oc FROM historico_dias_processados "
            "WHERE data_oc BETWEEN ? AND ?",
            (data_inicio, data_fim),
        ).fetchall()
        return {r["data_oc"] for r in rows}


def marcar_dia_processado(data_oc: str, tinha_dados: bool) -> None:
    """Marca um dia como ja consultado no backfill.
    Se `tinha_dados=False`, o dia nao sera re-consultado em execucoes
    futuras (otimizacao para dias vazios).
    """
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO historico_dias_processados
               (data_oc, tinha_dados, processado_em) VALUES (?, ?, ?)
               ON CONFLICT (data_oc) DO UPDATE SET
                 tinha_dados = EXCLUDED.tinha_dados,
                 processado_em = EXCLUDED.processado_em""",
            (data_oc, 1 if tinha_dados else 0, datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()


# ---------- R2 cross-time: cache de devoluções ----------

def atualizar_cache_devolucoes(linhas: list[dict[str, Any]]) -> int:
    """Substitui (TRUNCATE + INSERT) o cache local de devoluções abertas.
    `linhas` é uma lista de dicts com as chaves do schema. Retorna o
    número de linhas inseridas.

    Usa SAVEPOINT para garantir atomicidade: se o INSERT falhar, o
    DELETE é revertido e o cache antigo permanece intacto."""
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("SAVEPOINT cache_devolucoes_update")
        try:
            conn.execute("DELETE FROM cache_devolucoes")
            for l in linhas:
                conn.execute(
                    """INSERT INTO cache_devolucoes
                       (placa_normalizada, card_id, n_oc, peca_descricao,
                        fase_atual, atualizado_em)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        l.get("placa_normalizada"),
                        l.get("card_id"),
                        l.get("n_oc"),
                        l.get("peca_descricao"),
                        l.get("fase_atual"),
                        agora,
                    ),
                )
            conn.execute("RELEASE SAVEPOINT cache_devolucoes_update")
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT cache_devolucoes_update")
            raise
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
    chaves do schema. Retorna o número de linhas inseridas.

    Usa SAVEPOINT para garantir atomicidade: se o INSERT falhar, o
    DELETE é revertido e o cache antigo permanece intacto."""
    agora = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute("SAVEPOINT cache_cancelamentos_update")
        try:
            conn.execute("DELETE FROM cache_cancelamentos")
            for l in linhas:
                conn.execute(
                    """INSERT INTO cache_cancelamentos
                       (placa_normalizada, card_id, tipo, fase_atual,
                        descricao_pecas, codigo_oc, atualizado_em)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        l.get("placa_normalizada"),
                        l.get("card_id"),
                        l.get("tipo"),
                        l.get("fase_atual"),
                        l.get("descricao_pecas"),
                        l.get("codigo_oc"),
                        agora,
                    ),
                )
            conn.execute("RELEASE SAVEPOINT cache_cancelamentos_update")
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT cache_cancelamentos_update")
            raise
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


def listar_historico(
    limite: int = 30,
    data_inicio: str | None = None,
    data_fim: str | None = None,
) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    if data_inicio:
        where_parts.append("data_d1 >= ?")
        params.append(data_inicio)
    if data_fim:
        where_parts.append("data_d1 <= ?")
        params.append(data_fim)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    params.append(limite)

    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM validacoes {where_sql} ORDER BY data_execucao DESC LIMIT ?",
            tuple(params),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------- CRON locks ----------

def adquirir_cron_lock(
    data_d1: str,
    host: str,
    ttl_seconds: int,
    tentativa: int = 1,
) -> bool:
    """Tenta adquirir lock para executar o CRON do dia data_d1.

    Atomicidade via `INSERT ... ON CONFLICT DO UPDATE ... WHERE ... RETURNING`
    em 1 statement (SQLite 3.24+ e Postgres 9.5+). Elimina race TOCTOU.

    Retorna True se adquiriu (RETURNING devolveu linha). Retorna False se:
      - INSERT bateu ON CONFLICT E o UPDATE não se aplicou porque já existe
        lock válido ('sucesso'/'vazio'/'rodando' ainda no TTL).

    Regras de sobrescrita (cláusula WHERE no UPDATE):
      - 'falha': sempre libera (retry imediato).
      - 'rodando' ou 'sucesso'/'vazio' com expires_at < now: libera (TTL vencido).
      - Caso contrário: mantém lock atual, não adquire.
    """
    from datetime import timedelta
    agora = datetime.now()
    expires = agora + timedelta(seconds=ttl_seconds)
    agora_iso = agora.isoformat(timespec="seconds")
    expires_iso = expires.isoformat(timespec="seconds")

    # Usa rowcount em vez de RETURNING: o wrapper _dbconn trata RETURNING
    # esperando `id` inteiro para lastrowid. Com rowcount evitamos esse
    # caminho e mantemos atomicidade via ON CONFLICT + WHERE.
    sql = """
        INSERT INTO cron_locks
            (data_d1, acquired_at, expires_at, host, status, tentativa,
             last_error, updated_at)
        VALUES (?, ?, ?, ?, 'rodando', ?, NULL, ?)
        ON CONFLICT(data_d1) DO UPDATE SET
            acquired_at = excluded.acquired_at,
            expires_at  = excluded.expires_at,
            host        = excluded.host,
            status      = 'rodando',
            tentativa   = excluded.tentativa,
            last_error  = NULL,
            updated_at  = excluded.updated_at
        WHERE cron_locks.status = 'falha'
           OR cron_locks.expires_at < excluded.acquired_at
    """
    with get_conn() as conn:
        cur = conn.execute(
            sql,
            (data_d1, agora_iso, expires_iso, host, tentativa, agora_iso),
        )
        adquirido = cur.rowcount > 0
        conn.commit()
        return adquirido


def finalizar_cron_lock(
    data_d1: str,
    status: str,
    last_error: str | None = None,
) -> None:
    """Marca lock como 'sucesso' | 'vazio' | 'falha'. Truncar erro em 500."""
    if last_error and len(last_error) > 500:
        last_error = last_error[:500]
    agora_iso = datetime.now().isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """UPDATE cron_locks SET
                   status = ?, last_error = ?, updated_at = ?
               WHERE data_d1 = ?""",
            (status, last_error, agora_iso, data_d1),
        )
        conn.commit()


def ultimo_cron_lock() -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM cron_locks ORDER BY updated_at DESC, data_d1 DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


def ultima_falha_cron() -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM cron_locks WHERE status = 'falha' "
            "ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


def cron_pendente_de_execucao() -> dict[str, Any] | None:
    """Detecta se o CRON deveria ter executado hoje mas nao ha registro
    valido (cobre tres cenarios silenciosos):

      A) App dormiu (Render free) — APScheduler nunca disparou e nao
         existe linha em `cron_locks` para D-1.
      B) Lock travado em status='rodando' com expires_at < now (processo
         crashou no meio da execucao). Sem essa deteccao, o banner
         `ultima_falha` nao aparece (status nao eh 'falha').
      C) D-1 ja foi processado via fluxo manual (`POST /api/validar`)
         que nao escreve em `cron_locks`. Nesses casos NAO mostra banner.

    Retorna {"data_d1": "...", "horario_esperado": "HH:MM"} se:
      - Hoje ja passou da hora agendada do CRON em BRT
      - NAO ha registro valido em cron_locks para D-1 (rodando expirado
        conta como invalido)
      - NAO existe linha em `validacoes` para D-1

    Retorna None caso contrario, ou se CRON desabilitado.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from app.config import settings

    if not settings.cron_enabled:
        return None

    try:
        tz = ZoneInfo(settings.cron_timezone)
    except Exception:
        return None  # timezone mal configurada — nao bloqueia o endpoint

    agora = datetime.now(tz)
    horario_esperado_hoje = agora.replace(
        hour=settings.cron_hour_brt,
        minute=settings.cron_minute,
        second=0,
        microsecond=0,
    )
    if agora < horario_esperado_hoje:
        return None

    data_d1_esperada = (agora.date() - timedelta(days=1)).isoformat()
    agora_iso = agora.replace(tzinfo=None).isoformat(timespec="seconds")
    with get_conn() as conn:
        # Lock valido = qualquer status com expires_at no futuro,
        # OU status terminal (sucesso/vazio/falha) que ja registrou
        # tentativa — banners de falha/sucesso cobrem esses casos.
        # Lock 'rodando' com expires_at vencido NAO conta como valido.
        lock = conn.execute(
            "SELECT status, expires_at FROM cron_locks WHERE data_d1 = ?",
            (data_d1_esperada,),
        ).fetchone()
        if lock is not None:
            status = lock["status"] if hasattr(lock, "__getitem__") else lock[0]
            expires_at = lock["expires_at"] if hasattr(lock, "__getitem__") else lock[1]
            lock_valido = (
                status in ("sucesso", "vazio", "falha")
                or (status == "rodando" and expires_at and str(expires_at) > agora_iso)
            )
            if lock_valido:
                return None

        # Validacao manual (sem CRON) tambem cobre o D-1
        val = conn.execute(
            "SELECT 1 FROM validacoes WHERE data_d1 = ? LIMIT 1",
            (data_d1_esperada,),
        ).fetchone()
        if val is not None:
            return None

    return {
        "data_d1": data_d1_esperada,
        "horario_esperado": (
            f"{settings.cron_hour_brt:02d}:{settings.cron_minute:02d}"
        ),
    }


def dry_runs_cron_pendentes(dias: int = 3) -> list[dict[str, Any]]:
    """Validações CRON em dry-run dos últimos N dias ainda não aplicadas.

    Pendente = existe linha dry_run=1 origem='cron' sem linha posterior
    com dry_run=0 para o mesmo data_d1.

    Usa timezone BRT (America/Sao_Paulo) para calcular o corte, alinhado
    com `_computar_data_d1` no cron_runner. Se o servidor está em UTC,
    `date.today()` retornaria o dia errado na madrugada.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from app.config import settings
    tz = ZoneInfo(settings.cron_timezone)
    corte = (datetime.now(tz).date() - timedelta(days=dias)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT v.* FROM validacoes v
               WHERE v.dry_run = 1
                 AND v.origem = 'cron'
                 AND v.data_d1 >= ?
                 AND NOT EXISTS (
                     SELECT 1 FROM validacoes v2
                     WHERE v2.data_d1 = v.data_d1
                       AND v2.dry_run = 0
                       AND v2.data_execucao > v.data_execucao
                 )
               ORDER BY v.data_d1 DESC""",
            (corte,),
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
            for campo_json in ("regras_falhadas", "divergencias_json", "produtos_json"):
                if d.get(campo_json):
                    try:
                        d[campo_json] = json.loads(d[campo_json])
                    except (json.JSONDecodeError, TypeError):
                        pass
            result.append(d)
        return result
