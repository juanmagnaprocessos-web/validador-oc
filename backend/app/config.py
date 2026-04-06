"""Configuração centralizada via pydantic-settings.

Lê as variáveis do arquivo .env (ou do ambiente) e expõe um singleton `settings`.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

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

    # --- Cilia ---
    cilia_mode: str = Field("stub", alias="CILIA_MODE")  # stub | http
    cilia_api_url: str = Field("", alias="CILIA_API_URL")
    cilia_login: str = Field("", alias="CILIA_LOGIN")
    cilia_senha: str = Field("", alias="CILIA_SENHA")

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
    relatorios_dir: str = Field("relatorios", alias="RELATORIOS_DIR")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    r3_tolerancia_centavos: int = Field(0, alias="R3_TOLERANCIA_CENTAVOS")
    validador_identificador: str = Field(
        "validador-oc@magna", alias="VALIDADOR_IDENTIFICADOR"
    )

    @property
    def db_full_path(self) -> Path:
        p = BASE_DIR / self.db_path
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def relatorios_full_dir(self) -> Path:
        p = BASE_DIR / self.relatorios_dir
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def pipefy_ids_full_path(self) -> Path:
        return BASE_DIR / self.pipefy_ids_file

    @property
    def club_request_delay_s(self) -> float:
        return self.club_request_delay_ms / 1000.0


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
