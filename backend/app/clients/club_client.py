"""Cliente para a API privada do Club da Cotação.

A API foi mapeada via análise de tráfego — não há documentação pública.
Autentica via JWT e implementa retry/backoff + refresh automático.
Todos os endpoints confirmados em `api_clubdacotacao_instrucoes.md`.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import settings
from app.db import registrar_chamada_api
from app.logging_setup import get_logger
from app.utils.circuit_breaker import CircuitBreaker

logger = get_logger(__name__)


class ClubAuthError(Exception):
    """Falha ao autenticar no Club da Cotação."""


class ClubAPIError(Exception):
    """Erro genérico da API do Club."""


class ClubClient:
    """Cliente assíncrono com refresh de token e rate limiting suave."""

    def __init__(
        self,
        login: str | None = None,
        senha: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._login = login or settings.club_login
        self._senha = senha or settings.club_senha
        self._base_v1 = settings.club_api_base_v1
        self._base_v3 = settings.club_api_base_v3
        self._token: str | None = None
        self._last_request_at: float = 0.0
        self._breaker = CircuitBreaker("club", fail_threshold=5, reset_timeout=60)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": "validador-oc/0.1 (Magna Protecao)"},
        )

    # ---------- ciclo de vida ----------

    async def __aenter__(self) -> "ClubClient":
        await self.authenticate()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # ---------- autenticação ----------

    async def authenticate(self) -> str:
        """POST /v3/api/auth → armazena token JWT."""
        if not self._login or not self._senha:
            raise ClubAuthError(
                "CLUB_LOGIN/CLUB_SENHA não configurados. "
                "Copie .env.example para .env e preencha."
            )
        url = f"{self._base_v3}/auth"
        payload = {"usu_login": self._login, "senha": self._senha}

        started = time.perf_counter()
        try:
            resp = await self._client.post(url, json=payload)
        except httpx.RequestError as e:
            self._audit("POST", url, None, started, str(e))
            raise ClubAuthError(f"Falha de rede ao autenticar: {e}") from e

        self._audit("POST", url, resp.status_code, started)

        if resp.status_code != 200:
            raise ClubAuthError(
                f"Auth falhou — {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        # O campo do token pode variar: "token", "access_token" ou vir em data.token
        token = (
            data.get("token")
            or data.get("access_token")
            or (data.get("data") or {}).get("token")
        )
        if not token:
            raise ClubAuthError(
                f"Resposta de auth não contém token — keys: {list(data.keys())}"
            )

        self._token = token
        logger.info("Club: autenticado com sucesso")
        return token

    def _headers(self) -> dict[str, str]:
        if not self._token:
            raise ClubAuthError("Cliente não autenticado — chame authenticate() antes")
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    # ---------- throttling + retry ----------

    async def _throttle(self) -> None:
        """Respeita delay mínimo entre requests (rate limit preventivo)."""
        elapsed = time.monotonic() - self._last_request_at
        min_delay = settings.club_request_delay_s
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        self._last_request_at = time.monotonic()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Request protegida por circuit breaker."""
        return await self._breaker.call(
            self._do_request, method, url, params=params
        )

    async def _do_request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Request com retry exponencial e refresh de token em 401."""
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(settings.club_max_retries),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
            retry=retry_if_exception_type((httpx.RequestError, ClubAPIError)),
            reraise=True,
        ):
            with attempt:
                await self._throttle()
                started = time.perf_counter()
                try:
                    resp = await self._client.request(
                        method, url, params=params, headers=self._headers()
                    )
                except httpx.RequestError as e:
                    self._audit(method, url, None, started, str(e))
                    raise

                self._audit(method, url, resp.status_code, started)

                if resp.status_code == 401:
                    logger.warning("Club: 401 — reautenticando e retentando")
                    await self.authenticate()
                    raise ClubAPIError("401 — token renovado, retry")

                if resp.status_code == 429 or resp.status_code >= 500:
                    raise ClubAPIError(
                        f"{resp.status_code} em {url}: {resp.text[:200]}"
                    )

                if resp.status_code >= 400:
                    # 4xx não-auth: não adianta retentar
                    raise ClubAPIError(
                        f"{resp.status_code} em {url}: {resp.text[:300]}"
                    )

                return resp.json()

        raise ClubAPIError(f"Falha apos retries em {url}")  # unreachable

    def _audit(
        self,
        method: str,
        url: str,
        status: int | None,
        started: float,
        erro: str | None = None,
    ) -> None:
        duracao = int((time.perf_counter() - started) * 1000)
        try:
            registrar_chamada_api("club", method, url, status, duracao, erro)
        except Exception as e:  # auditoria nunca deve quebrar a chamada
            logger.debug("Falha ao auditar: %s", e)

    # ---------- endpoints de negócio ----------

    async def listar_pedidos(
        self,
        data_d1: date,
        *,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        """GET /api/listarpedidos?datainicial=D-1&datafinal=D-1&total=true"""
        url = f"{self._base_v1}/listarpedidos"
        params = {
            "page": page,
            "datainicial": data_d1.strftime("%Y-%m-%d"),
            "datafinal": data_d1.strftime("%Y-%m-%d"),
            "identifier": "",
            "singleorder": "false",
            "total": "true",
        }
        data = await self._request("GET", url, params=params)
        # A resposta pode vir como { "pedidos": [...] } ou como lista direta
        pedidos = data.get("pedidos") if isinstance(data, dict) else data
        if pedidos is None and isinstance(data, dict):
            # fallback: procurar qualquer lista dentro do objeto
            for v in data.values():
                if isinstance(v, list):
                    pedidos = v
                    break
        return pedidos or []

    async def get_concorrentes(self, id_cotacao: str | int) -> list[dict[str, Any]]:
        """GET /api/getconcorrentescotacao?numerocotacao={id} — usado em R1."""
        url = f"{self._base_v1}/getconcorrentescotacao"
        data = await self._request(
            "GET", url, params={"numerocotacao": str(id_cotacao)}
        )
        return data.get("concorrentes", []) if isinstance(data, dict) else []

    async def get_produtos_cotacao(
        self, id_cotacao: str | int
    ) -> list[dict[str, Any]]:
        """GET /api/getprodutoscotacao?cotacao={id} — usado em R2."""
        url = f"{self._base_v1}/getprodutoscotacao"
        data = await self._request("GET", url, params={"cotacao": str(id_cotacao)})
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("produtos", "items", "data"):
                if isinstance(data.get(k), list):
                    return data[k]
        return []

    async def get_order_details(self, id_pedido: str | int) -> dict[str, Any]:
        """GET /v3/api/clients/orders/{id} — detalhes completos com items[]."""
        url = f"{self._base_v3}/clients/orders/{id_pedido}"
        return await self._request("GET", url)

    async def get_valor_oc(self, id_pedido: str | int) -> Decimal | None:
        """Helper para R3 — extrai apenas o valor total."""
        dados = await self.get_order_details(id_pedido)
        valor = dados.get("value") or dados.get("valor_pedido")
        if valor is None:
            # soma dos items se valor global não existir
            items = dados.get("items") or []
            total = Decimal("0")
            for it in items:
                tp = it.get("total_price") or 0
                try:
                    total += Decimal(str(tp))
                except Exception:
                    pass
            return total if total > 0 else None
        try:
            return Decimal(str(valor))
        except Exception:
            return None

    async def listar_fornecedores(self) -> list[dict[str, Any]]:
        """GET /api/getfornecedorescliente — referência para R5."""
        url = f"{self._base_v1}/getfornecedorescliente"
        data = await self._request("GET", url)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("fornecedores", "data", "items"):
                if isinstance(data.get(k), list):
                    return data[k]
        return []
