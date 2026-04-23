"""Rate limit + log persistente de tentativas de autenticacao.

Objetivo: proteger `get_current_user` (HTTP Basic Auth em TODAS as rotas)
contra brute-force sem depender de biblioteca externa. Storage em SQL
(tabela `login_attempts`) garante persistencia atraves de reinicios —
Render Free hiberna apos 15min de inatividade; solucoes in-memory
(slowapi default) zerariam o contador e seriam bypassaveis.

Limites (configuraveis via env):
  * 5 tentativas/60s por (IP, username)  — anti brute-force focado
  * 20 tentativas/60s por IP global      — anti username-spray
  * Ambos contam so falhas (resultado != 'sucesso').

Defesas adicionais:
  * IPv6 normalizado para /64 (impede queima de /64 inteiro por atacante
    com VPS IPv6).
  * user_agent truncado em 500 chars (anti DoS via log bloat).
  * Dummy hash constante para timing constante quando usuario nao existe.
  * `unlock` por CLI para self-lockout de admin.
"""
from __future__ import annotations

import ipaddress
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import bcrypt
from fastapi import HTTPException, Request, status

from app.config import settings
from app.db import (
    contar_falhas_recentes,
    purgar_tentativas_login_antigas,
    registrar_tentativa_login,
    unlock_tentativas_login,
)

logger = logging.getLogger(__name__)

# Hash dummy gerado uma vez por processo. Custo de gerar na inicializacao
# (~200ms) ocorre so no startup. bcrypt.checkpw contra ele da timing
# indistinguivel de verificar senha real quando usuario existe.
DUMMY_HASH = bcrypt.hashpw(b"dummy-never-matches", bcrypt.gensalt()).decode("ascii")

_UA_MAX_LEN = 500

Resultado = Literal[
    "sucesso",
    "senha_errada",
    "usuario_inexistente",
    "usuario_desativado",
    "rate_limited_ip",
    "rate_limited_usuario",
    "credenciais_ausentes",
]


def _now_iso() -> str:
    """Timestamp ISO-8601 UTC com segundos. Centralizado para facilitar
    mock em testes via monkeypatch + freezegun."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def extrair_ip_real(request: Request) -> str:
    """Extrai o IP real do cliente atras do proxy Render.

    MODELO DE CONFIANCA (IMPORTANTE):
      * Deploy atual: Render Free. Render e o UNICO caminho de ingresso —
        nao existe rota TCP direta pro app. Todo request passa pelo proxy
        deles, que injeta `X-Forwarded-For` com o IP real do cliente na
        POSICAO 0 (documentado por Render).
      * Per docs Render: quando o cliente envia seu proprio XFF, Render
        MANTEM o valor deles como PRIMEIRO da cadeia — qualquer XFF que
        o cliente mande vai pras posicoes seguintes. Por isso
        `xff.split(",")[0]` eh seguro AQUI. Em qualquer outro provedor,
        revise essa premissa.
      * Se for migrar de Render: ou (a) configurar uvicorn com
        `--proxy-headers --forwarded-allow-ips=<ips_do_proxy>`, ou
        (b) reescrever este extrator pra tomar a entrada RIGHTMOST do XFF.

    Regras de extracao:
      1. `X-Forwarded-For[0]` se presente.
      2. Fallback: `request.client.host` (dev local ou sem proxy).
      3. Fallback final: "0.0.0.0" (evita None quebrando SQL).

    IPv6: normalizado para /64 — um unico cliente em casa com VPS IPv6
    recebe /64 inteiro e poderia queimar 2^64 IPs distintos bypassando
    rate limit por IP exato.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # Primeiro valor = client real (Render e proxies confiaveis injetam
        # no formato "real_ip, proxy1, proxy2")
        primeiro = xff.split(",")[0].strip()
        if primeiro:
            return _normalizar_ip(primeiro)
    # Fallback: starlette/uvicorn ja resolve request.client quando
    # forwarded_allow_ips=* esta configurado no main.
    client = request.client
    if client and client.host:
        return _normalizar_ip(client.host)
    return "0.0.0.0"


def _normalizar_ip(ip: str) -> str:
    """Normaliza IPv6 para /64 (mantem prefixo, zera restante). IPv4 intacto.

    Tambem strip de colchetes (`[::1]:1234`) e porta se vierem juntos.
    """
    raw = ip.strip()
    # Strip port se formato "ip:port" (IPv4) — IPv6 usa colchetes
    if raw.startswith("["):
        fechou = raw.find("]")
        if fechou > 0:
            raw = raw[1:fechou]
    elif raw.count(":") == 1:
        raw = raw.split(":")[0]
    try:
        addr = ipaddress.ip_address(raw)
    except ValueError:
        return raw  # formato invalido — salva raw pra auditoria
    if isinstance(addr, ipaddress.IPv6Address):
        net = ipaddress.IPv6Network(f"{addr}/64", strict=False)
        return str(net.network_address)
    return str(addr)


def _truncar_ua(ua: str | None) -> str | None:
    if ua is None:
        return None
    if len(ua) <= _UA_MAX_LEN:
        return ua
    return ua[:_UA_MAX_LEN]


def _desde_iso(janela_s: int, *, agora: datetime | None = None) -> str:
    base = agora or datetime.now(timezone.utc)
    return (base - timedelta(seconds=janela_s)).replace(microsecond=0).isoformat()


def registrar_tentativa(
    request: Request | None,
    username: str,
    resultado: Resultado,
    *,
    ip: str | None = None,
    rota: str | None = None,
) -> None:
    """Wrapper sobre o helper de DB: resolve IP + UA + timestamp.

    `request` pode ser None em casos de teste isolado, nesse caso `ip`
    precisa vir explicito.
    """
    if ip is None:
        if request is None:
            ip = "0.0.0.0"
        else:
            ip = extrair_ip_real(request)
    ua = None
    if request is not None:
        ua = _truncar_ua(request.headers.get("user-agent"))
    if rota is None and request is not None:
        try:
            rota = request.url.path
        except Exception:
            rota = None
    try:
        registrar_tentativa_login(
            ts=_now_iso(),
            ip=ip,
            username=username or "",
            user_agent=ua,
            resultado=resultado,
            rota=rota,
        )
    except Exception:
        # Falha ao logar nunca deve quebrar autenticacao — apenas logamos
        # em stdout pra debug. Exemplo: Neon offline no momento do login.
        logger.exception(
            "Falha ao registrar login_attempts (resultado=%s, user=%s)",
            resultado, username,
        )


def checar_rate_limit(request: Request, username: str) -> None:
    """Aplica rate limit antes da verificacao de senha. Raise 429 se excedeu.

    Checa DOIS limites independentes: por IP global + por (IP, username).
    O limite por IP dispara primeiro quando atacante troca de username;
    o composto dispara primeiro em brute-force focado.

    Side effect: se dispara, registra linha com resultado apropriado antes
    de levantar HTTPException.

    LIMITACAO CONHECIDA (aceita como trade-off):
      SELECT-then-INSERT nao e atomico. Sob concorrencia alta (>5 req/s),
      um burst pode ultrapassar o teto em algumas tentativas antes do
      contador estabilizar. Em app de auth com bcrypt (~200ms por
      checkpw), o GIL + latencia de rede naturalmente serializam — o
      burst real observado e ~1-2 tentativas a mais. Mitigacao se
      virar problema: introduzir UNIQUE constraint em
      (ip, username, bucket_minute) + INSERT OR IGNORE, ou usar
      SERIALIZABLE isolation em Postgres. Proximo PR.
    """
    if not settings.login_rate_enabled:
        return

    ip = extrair_ip_real(request)
    janela = settings.login_rate_janela_s
    desde = _desde_iso(janela)

    # Limite por IP (global)
    falhas_ip = contar_falhas_recentes(ip=ip, username=None, desde_iso=desde)
    if falhas_ip >= settings.login_rate_ip_max:
        registrar_tentativa(
            request, username, "rate_limited_ip", ip=ip,
        )
        logger.warning(
            "Rate limit IP atingido: ip=%s falhas=%d/%d janela=%ds",
            ip, falhas_ip, settings.login_rate_ip_max, janela,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Muitas tentativas. Tente novamente em instantes.",
            headers={"Retry-After": str(janela)},
        )

    # Limite composto (IP + username) — so se username nao-vazio
    if username:
        falhas_user = contar_falhas_recentes(
            ip=ip, username=username, desde_iso=desde,
        )
        if falhas_user >= settings.login_rate_ip_user_max:
            registrar_tentativa(
                request, username, "rate_limited_usuario", ip=ip,
            )
            logger.warning(
                "Rate limit IP+user atingido: ip=%s user=%s falhas=%d/%d",
                ip, username, falhas_user, settings.login_rate_ip_user_max,
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Muitas tentativas. Tente novamente em instantes.",
                headers={"Retry-After": str(janela)},
            )


def consumir_bcrypt_dummy(senha: str) -> None:
    """Queima ~200ms verificando bcrypt contra hash dummy. Usado quando
    o username nao existe — mantem tempo de resposta indistinguivel de
    username valido com senha errada, bloqueando enumeracao de usuarios.

    Usa a mesma logica de truncate que `verificar_senha` em services/auth.py
    para manter consistencia de tempo (maior senha = mais custo de bytes).
    """
    from app.services.auth import _to_bytes
    try:
        bcrypt.checkpw(_to_bytes(senha), DUMMY_HASH.encode("ascii"))
    except Exception:
        pass


def unlock(
    *,
    username: str | None = None,
    ip: str | None = None,
) -> int:
    """Wrapper amigavel do helper de DB pra uso via CLI."""
    return unlock_tentativas_login(username=username, ip=ip)


def purgar_logs_antigos() -> dict[str, Any]:
    """Deleta tentativas mais antigas que `LOGIN_ATTEMPTS_RETENTION_DAYS`.
    Pode ser chamado por CLI ou job futuro."""
    corte = datetime.now(timezone.utc) - timedelta(
        days=settings.login_attempts_retention_days
    )
    corte_iso = corte.replace(microsecond=0).isoformat()
    removidos = purgar_tentativas_login_antigas(ate_iso=corte_iso)
    return {
        "removidos": removidos,
        "corte_iso": corte_iso,
        "retention_days": settings.login_attempts_retention_days,
    }
