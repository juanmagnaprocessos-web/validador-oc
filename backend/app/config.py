"""Configuração centralizada via pydantic-settings.

Lê as variáveis do arquivo .env (ou do ambiente) e expõe um singleton `settings`.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Club da Cotação ---
    # Vazio por default para permitir import sem .env (ex: --help, docs).
    # Validação real acontece quando o ClubClient tenta autenticar.
    club_login: str = Field("", alias="CLUB_LOGIN")
    club_senha: str = Field("", alias="CLUB_SENHA")
    club_api_base_v1: str = Field(
        "https://api.clubdacotacao.com.br/api", alias="CLUB_API_BASE_V1"
    )
    club_api_base_v3: str = Field(
        "https://api.clubdacotacao.com.br/v3/api", alias="CLUB_API_BASE_V3"
    )
    club_request_delay_ms: int = Field(300, alias="CLUB_REQUEST_DELAY_MS")
    club_max_retries: int = Field(5, alias="CLUB_MAX_RETRIES")

    # --- Pipefy ---
    pipefy_token: str = Field("", alias="PIPEFY_TOKEN")
    pipefy_api_url: str = Field(
        "https://api.pipefy.com/graphql", alias="PIPEFY_API_URL"
    )
    pipe_id: int = Field(305587531, alias="PIPE_ID")
    pipefy_ids_file: str = Field("config/pipefy_ids.json", alias="PIPEFY_IDS_FILE")
    # Pipe de "Devolução de Peças" — consultado pela R2 cross-time para
    # saber se uma peça reincidente já tem devolução em aberto (caso em
    # que a re-compra é legítima e o alerta é leve).
    pipefy_pipe_devolucao_id: int = Field(
        305658860, alias="PIPEFY_PIPE_DEVOLUCAO_ID"
    )
    # Tamanho da página para queries de listagem (cards por request).
    # Pipefy aceita até 100; valor maior = menos chamadas API.
    pipefy_page_size: int = Field(100, ge=1, le=100, alias="PIPEFY_PAGE_SIZE")

    # --- Cilia ---
    # Modos disponíveis:
    #   stub     = dados sintéticos (default seguro, sem rede)
    #   http     = cliente HTTP real com login automático + cookie persistente
    #   deeplink = não consulta o Cilia, só renderiza link clicável no relatório
    #              para o analista validar manualmente
    #   off      = não usa Cilia, não exibe coluna no relatório
    cilia_mode: Literal["stub", "http", "deeplink", "off"] = Field(
        "stub", alias="CILIA_MODE"
    )
    cilia_base_url: str = Field(
        "https://sistema.cilia.com.br", alias="CILIA_BASE_URL"
    )
    cilia_api_url: str = Field("", alias="CILIA_API_URL")  # legado, mantido para compat
    cilia_login: str = Field("", alias="CILIA_LOGIN")
    cilia_senha: str = Field("", alias="CILIA_SENHA")
    # Cookie de sessão persistido entre execuções (idade < 23h)
    cilia_session_file: str = Field(
        "data/cilia_session.json", alias="CILIA_SESSION_FILE"
    )
    # Cache de orçamentos por placa (TTL em segundos, default 4h)
    cilia_cache_ttl_s: int = Field(14400, alias="CILIA_CACHE_TTL_S")
    # Delay mínimo entre requisições ao Cilia (rate limit defensivo)
    cilia_request_delay_ms: int = Field(1000, alias="CILIA_REQUEST_DELAY_MS")
    # Janela de busca em /api/surveys/search.json (filtro obrigatório)
    cilia_search_janela_dias: int = Field(90, alias="CILIA_SEARCH_JANELA_DIAS")

    # --- SMTP ---
    smtp_host: str = Field("smtp.gmail.com", alias="SMTP_HOST")
    smtp_port: int = Field(587, alias="SMTP_PORT")
    smtp_user: str = Field("", alias="SMTP_USER")
    smtp_senha: str = Field("", alias="SMTP_SENHA")
    smtp_remetente: str = Field("", alias="SMTP_REMETENTE")
    email_enabled: bool = Field(False, alias="EMAIL_ENABLED")

    # --- Aplicação ---
    app_env: str = Field("development", alias="APP_ENV")
    db_path: str = Field("data/validador.db", alias="DB_PATH")
    # DATABASE_URL (quando presente) tem prioridade sobre db_path.
    # Aceita:
    #   - "" (vazio)                    -> usa SQLite em data/db_path
    #   - "sqlite:///caminho/arq.db"    -> SQLite no caminho indicado
    #   - "postgresql://user:pass@host/db?sslmode=require" -> Postgres (Neon)
    database_url: str = Field("", alias="DATABASE_URL")
    relatorios_dir: str = Field("relatorios", alias="RELATORIOS_DIR")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    r3_tolerancia_centavos: int = Field(0, alias="R3_TOLERANCIA_CENTAVOS")
    validador_identificador: str = Field(
        "validador-oc@magna", alias="VALIDADOR_IDENTIFICADOR"
    )

    # --- Modo de operação ---
    # "consulta": sistema NÃO escreve em sistemas externos (Pipefy/e-mail).
    #             Toda ação que SERIA tomada é registrada em
    #             `acoes_pipefy_planejadas` para auditoria. Default seguro
    #             durante a fase de validação manual.
    # "automatico": ações reais são aplicadas no Pipefy e e-mails enviados.
    #               Só ligar quando a confiança no sistema permitir.
    modo_operacao: Literal["consulta", "automatico"] = Field(
        "consulta", alias="MODO_OPERACAO"
    )

    # --- R2 (peça repetida cruzada) ---
    r2_janela_dias: int = Field(210, alias="R2_JANELA_DIAS")
    # Modo da verificação cross-time:
    #   "alerta"   = só sinaliza no relatório (default seguro)
    #   "bloqueio" = gera Severidade.ERRO e move o card
    #   "off"      = desliga completamente a verificação cross-time
    r2_modo: Literal["alerta", "bloqueio", "off"] = Field(
        "alerta", alias="R2_MODO"
    )
    # Fonte do historico para R2 cross-time:
    #   "pipefy" = indexa cards do pipe principal + busca detalhes no Club
    #              sob demanda (default — nao depende de backfill e funciona
    #              em Render Free, que nao converge o backfill Club).
    #   "sqlite" = usa tabela local `historico_produtos_oc` populada via
    #              `garantir_historico` (legado — so funciona se o backfill
    #              tiver rodado com time_budget suficiente).
    r2_fonte_historico: Literal["pipefy", "sqlite"] = Field(
        "pipefy", alias="R2_FONTE_HISTORICO"
    )
    pipefy_fases_cancelamento: str = Field(
        "Cancelados,Informações Incorretas",
        alias="PIPEFY_FASES_CANCELAMENTO",
    )

    # --- CRON diário ---
    # Quando habilitado, APScheduler inicia junto com FastAPI e dispara
    # `run_daily_validation_job` todo dia às `cron_hour_brt:cron_minute`
    # (timezone BRT explícito). Lock persistente em `cron_locks` impede
    # dupla execução. Dry-run por default — analista confere e aplica.
    cron_enabled: bool = Field(False, alias="CRON_ENABLED")
    cron_hour_brt: int = Field(2, ge=0, le=23, alias="CRON_HOUR_BRT")
    cron_minute: int = Field(0, ge=0, le=59, alias="CRON_MINUTE")
    cron_timezone: str = Field("America/Sao_Paulo", alias="CRON_TIMEZONE")
    cron_misfire_grace_s: int = Field(3600, alias="CRON_MISFIRE_GRACE_S")
    cron_lock_ttl_s: int = Field(7200, alias="CRON_LOCK_TTL_S")  # 2h
    cron_retry_delays_min: str = Field("15,45", alias="CRON_RETRY_DELAYS_MIN")
    cron_dry_run: bool = Field(True, alias="CRON_DRY_RUN")

    # --- CORS ---
    cors_origins: str = Field(
        "http://localhost:5173", alias="CORS_ORIGINS"
    )  # CSV

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def fases_cancelamento_list(self) -> list[str]:
        return [
            f.strip() for f in self.pipefy_fases_cancelamento.split(",") if f.strip()
        ]

    @property
    def db_full_path(self) -> Path:
        raw = Path(self.db_path)
        p = raw if raw.is_absolute() else BASE_DIR / raw
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def db_dialect(self) -> str:
        """Retorna 'postgres' ou 'sqlite' conforme DATABASE_URL."""
        url = self.database_url.strip().lower()
        if url.startswith(("postgres://", "postgresql://")):
            return "postgres"
        return "sqlite"

    @property
    def db_connection_string(self) -> str:
        """URL/path final de conexao. Para SQLite retorna o caminho absoluto."""
        if self.db_dialect == "postgres":
            return self.database_url
        # Se DATABASE_URL = sqlite:///path, extrai; senao usa db_full_path
        url = self.database_url.strip()
        if url.startswith("sqlite:///"):
            return url[len("sqlite:///"):]
        return str(self.db_full_path)

    @property
    def relatorios_full_dir(self) -> Path:
        raw = Path(self.relatorios_dir)

        if raw.is_absolute():
            # Caminho absoluto explícito via ENV sempre tem prioridade.
            p = raw
        else:
            # Em produção no Render, o filesystem da aplicação é efêmero — cada
            # redeploy apaga o que foi escrito lá. O render.yaml declara um
            # disco persistente de 1GB montado em /data. Se estamos rodando em
            # produção E /data existe, usar /data/<relatorios_dir>. Caso
            # contrário (dev local, teste), seguir com BASE_DIR relativo.
            data_disk = Path("/data")
            if self.app_env == "production" and data_disk.exists() and data_disk.is_dir():
                p = data_disk / raw
            else:
                p = BASE_DIR / raw

        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def pipefy_ids_full_path(self) -> Path:
        return BASE_DIR / self.pipefy_ids_file

    @property
    def club_request_delay_s(self) -> float:
        return self.club_request_delay_ms / 1000.0

    @property
    def cron_retry_delays_list(self) -> list[int]:
        return [
            int(x.strip())
            for x in self.cron_retry_delays_min.split(",")
            if x.strip().isdigit()
        ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
