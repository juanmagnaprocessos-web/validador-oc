-- Schema Postgres (Neon) do validador-oc.
-- Convertido do schema SQLite em db.py mantendo mesma semantica:
--   * INTEGER PRIMARY KEY AUTOINCREMENT -> BIGSERIAL PRIMARY KEY
--   * REAL                              -> DOUBLE PRECISION
--   * COLLATE NOCASE em usuarios.username -> removido (queries usam LOWER())
--   * CURRENT_TIMESTAMP e TEXT p/ datas continuam iguais
--   * UNIQUE, INDEX e FOREIGN KEY mantidos
-- Timestamps sao armazenados como TEXT ISO (alinhado com o SQLite) para
-- evitar reescrever codigo de leitura. Migracao futura para
-- TIMESTAMPTZ pode ser feita separadamente.

CREATE TABLE IF NOT EXISTS validacoes (
    id              BIGSERIAL PRIMARY KEY,
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
    executado_por   TEXT,
    aguardando_ml   INTEGER NOT NULL DEFAULT 0,
    ja_processadas  INTEGER NOT NULL DEFAULT 0,
    origem          TEXT    NOT NULL DEFAULT 'manual'
);

CREATE INDEX IF NOT EXISTS idx_validacoes_data_d1 ON validacoes(data_d1);

CREATE TABLE IF NOT EXISTS oc_resultados (
    id                       BIGSERIAL PRIMARY KEY,
    validacao_id             INTEGER NOT NULL,
    id_pedido                TEXT    NOT NULL,
    id_cotacao               TEXT,
    placa                    TEXT,
    placa_normalizada        TEXT,
    fornecedor               TEXT,
    comprador                TEXT,
    forma_pagamento          TEXT,
    valor_card               DOUBLE PRECISION,
    valor_club               DOUBLE PRECISION,
    valor_pdf                DOUBLE PRECISION,
    valor_cilia              DOUBLE PRECISION,
    qtd_cotacoes             INTEGER,
    qtd_produtos             INTEGER,
    peca_duplicada           TEXT,
    status                   TEXT NOT NULL,
    regras_falhadas          TEXT,
    fase_pipefy              TEXT,
    fase_pipefy_atual        TEXT,
    card_pipefy_id           TEXT,
    divergencias_json        TEXT,
    produtos_json            TEXT,
    reincidencia             TEXT DEFAULT '—',
    cancelamento             TEXT DEFAULT '—',
    cancelamento_card_id     TEXT,
    card_pipefy_link         TEXT,
    forma_pagamento_canonica TEXT,
    FOREIGN KEY (validacao_id) REFERENCES validacoes(id)
);

CREATE INDEX IF NOT EXISTS idx_oc_resultados_validacao ON oc_resultados(validacao_id);
CREATE INDEX IF NOT EXISTS idx_oc_resultados_placa ON oc_resultados(placa_normalizada);

CREATE TABLE IF NOT EXISTS auditoria_api (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    TEXT NOT NULL,
    sistema      TEXT NOT NULL,
    metodo       TEXT NOT NULL,
    url          TEXT NOT NULL,
    status_code  INTEGER,
    duracao_ms   INTEGER,
    erro         TEXT
);

CREATE INDEX IF NOT EXISTS idx_auditoria_timestamp ON auditoria_api(timestamp);

CREATE TABLE IF NOT EXISTS acoes_pipefy_planejadas (
    id            BIGSERIAL PRIMARY KEY,
    validacao_id  INTEGER NOT NULL,
    oc_numero     TEXT    NOT NULL,
    card_id       TEXT,
    acao          TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    motivo        TEXT,
    executada     INTEGER NOT NULL DEFAULT 0,
    erro          TEXT,
    criado_em     TEXT    NOT NULL,
    FOREIGN KEY (validacao_id) REFERENCES validacoes(id)
);

CREATE INDEX IF NOT EXISTS idx_acoes_validacao ON acoes_pipefy_planejadas(validacao_id);
CREATE INDEX IF NOT EXISTS idx_acoes_card ON acoes_pipefy_planejadas(card_id);

CREATE TABLE IF NOT EXISTS perfis (
    id          BIGSERIAL PRIMARY KEY,
    nome        TEXT    UNIQUE NOT NULL,
    descricao   TEXT,
    permissoes  TEXT    NOT NULL DEFAULT '[]',
    criado_em   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS usuarios (
    id                     BIGSERIAL PRIMARY KEY,
    username               TEXT    UNIQUE NOT NULL,
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

CREATE TABLE IF NOT EXISTS historico_produtos_oc (
    id                 BIGSERIAL PRIMARY KEY,
    data_oc            TEXT    NOT NULL,
    id_pedido          TEXT    NOT NULL,
    id_cotacao         TEXT,
    placa_normalizada  TEXT    NOT NULL,
    identificador      TEXT,
    chave_produto      TEXT    NOT NULL,
    descricao          TEXT,
    fornecedor_id      TEXT,
    fornecedor_nome    TEXT,
    quantidade         DOUBLE PRECISION,
    card_pipefy_id     TEXT,
    criado_em          TEXT    NOT NULL,
    UNIQUE(id_pedido, chave_produto)
);

CREATE INDEX IF NOT EXISTS idx_hist_placa_data
    ON historico_produtos_oc(placa_normalizada, data_oc);
CREATE INDEX IF NOT EXISTS idx_hist_data
    ON historico_produtos_oc(data_oc);

CREATE TABLE IF NOT EXISTS historico_dias_processados (
    data_oc       TEXT    PRIMARY KEY,
    tinha_dados   INTEGER NOT NULL DEFAULT 0,
    processado_em TEXT    NOT NULL
);

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

CREATE TABLE IF NOT EXISTS cache_cancelamentos (
    placa_normalizada  TEXT    NOT NULL,
    card_id            TEXT    NOT NULL,
    tipo               TEXT    NOT NULL,
    fase_atual         TEXT,
    descricao_pecas    TEXT,
    codigo_oc          TEXT,
    atualizado_em      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_canc_placa
    ON cache_cancelamentos(placa_normalizada);

CREATE TABLE IF NOT EXISTS cache_cilia_orcamentos (
    placa_normalizada  TEXT    PRIMARY KEY,
    encontrado         INTEGER NOT NULL,
    payload_json       TEXT    NOT NULL,
    atualizado_em      TEXT    NOT NULL
);

-- Tabela auxiliar para resolver comprador a partir do club_user_id
CREATE TABLE IF NOT EXISTS compradores (
    club_user_id BIGINT PRIMARY KEY,
    nome         TEXT NOT NULL,
    email        TEXT NOT NULL,
    ativo        INTEGER NOT NULL DEFAULT 1,
    criado_em    TEXT DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Lock persistente do CRON diário. PK = data_d1 garante unicidade.
-- Protege contra dupla execução entre workers/restarts. TTL via expires_at.
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
