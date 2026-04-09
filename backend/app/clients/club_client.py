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
        """GET /api/listarpedidos?datainicial=D-1&datafinal=D-1&total=true (v1 — fallback)"""
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

    async def listar_pedidos_v3(
        self,
        data_inicio: date,
        data_fim: date | None = None,
        *,
        products: bool = True,
        seller: bool = True,
        buyer: bool = True,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /v3/api/clients/orders com paginação automática.

        Retorna TODAS as OCs do período com items, fornecedor e comprador
        inline (quando solicitados), eliminando chamadas individuais a
        get_order_details.

        A resposta v3 usa nomes em inglês:
          - id, number_quote, identifier, value, generation_date, status
          - items[].product.{id, name, ean, internal_code}
          - items[].{quantity, unit_price, total_price}
          - seller.{id, name, cnpj, status, excluded}
          - buyer.{id, name, email}

        Para compatibilidade com _parse_oc (orchestrator), cada pedido é
        normalizado com aliases v1 (id_pedido, id_cotacao, fornecedor, etc.)
        via _normalizar_pedido_v3.
        """
        if data_fim is None:
            data_fim = data_inicio

        url = f"{self._base_v3}/clients/orders"
        todos: list[dict[str, Any]] = []
        page = 1

        while True:
            params: dict[str, Any] = {
                "from": data_inicio.strftime("%Y-%m-%d"),
                "to": data_fim.strftime("%Y-%m-%d"),
                "page": page,
            }
            if products:
                params["products"] = 1
            if seller:
                params["seller"] = 1
            if buyer:
                params["buyer"] = 1
            if status:
                params["status"] = status

            data = await self._request("GET", url, params=params)

            # Extrair a lista de pedidos da resposta
            pedidos: list[dict[str, Any]] = []
            if isinstance(data, list):
                pedidos = data
            elif isinstance(data, dict):
                # v3 pode retornar { "data": [...] } ou { "orders": [...] }
                pedidos = (
                    data.get("data")
                    or data.get("orders")
                    or data.get("pedidos")
                    or []
                )
                if not pedidos and not isinstance(pedidos, list):
                    # fallback: qualquer lista
                    for v in data.values():
                        if isinstance(v, list):
                            pedidos = v
                            break

            if not pedidos:
                break

            # Normalizar cada pedido para compatibilidade com o restante do sistema
            for p in pedidos:
                todos.append(self._normalizar_pedido_v3(p))

            # Se retornou menos que uma página cheia, acabou.
            # Heurística: se a API retornar exatamente 0, paramos.
            # Se não soubermos o tamanho da página, usamos 20 como padrão
            # e paramos se vier menos.
            page_size = 20
            if isinstance(data, dict):
                page_size = data.get("per_page") or data.get("limit") or 20
            if len(pedidos) < page_size:
                break

            page += 1

        logger.info(
            "Club v3: %d OCs listadas de %s a %s (%d páginas)",
            len(todos), data_inicio, data_fim, page,
        )
        return todos

    @staticmethod
    def _normalizar_pedido_v3(raw: dict[str, Any]) -> dict[str, Any]:
        """Adiciona aliases v1 a um pedido retornado pela API v3 para que
        _parse_oc (orchestrator) e _coletar_dia (historico) funcionem sem
        alteração.

        Os campos originais v3 são PRESERVADOS — apenas adicionamos
        aliases quando o campo v1 correspondente não existe.
        """
        # --- IDs ---
        if "id_pedido" not in raw and "id" in raw:
            raw["id_pedido"] = raw["id"]
        if "id_cotacao" not in raw and "number_quote" in raw:
            raw["id_cotacao"] = raw["number_quote"]

        # --- Identificador / placa ---
        if "identificador" not in raw and "identifier" in raw:
            raw["identificador"] = raw["identifier"]

        # --- Valor ---
        if "valor_pedido" not in raw and "value" in raw:
            raw["valor_pedido"] = raw["value"]

        # --- Data ---
        if "data_pedido" not in raw and "generation_date" in raw:
            raw["data_pedido"] = raw["generation_date"]

        # --- Fornecedor (seller → fornecedor dict com for_id/for_nome) ---
        seller = raw.get("seller") or {}
        if seller and "fornecedor" not in raw:
            raw["fornecedor"] = {
                "for_id": seller.get("id"),
                "for_nome": seller.get("name"),
                "for_cnpj": seller.get("cnpj"),
                "for_status": seller.get("status"),
                "for_excluido": seller.get("excluded") or "0",
            }
            # for_id de nível raiz (usado por _parse_oc)
            if "for_id" not in raw and seller.get("id"):
                raw["for_id"] = seller["id"]

        # --- Comprador (buyer) ---
        buyer = raw.get("buyer") or {}
        if buyer:
            if "created_by" not in raw and buyer.get("id"):
                raw["created_by"] = buyer["id"]
            if "usu_nome" not in raw and buyer.get("name"):
                raw["usu_nome"] = buyer["name"]

        # --- Items já vêm no formato v3 (product.name, quantity, etc.) ---
        # _parse_oc já sabe ler esse formato, nada a fazer.

        return raw

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
