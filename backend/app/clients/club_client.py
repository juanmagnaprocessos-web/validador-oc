"""Cliente para a API privada do Club da Cotação.

A API foi mapeada via análise de tráfego — não há documentação pública.
Autentica via JWT e implementa retry/backoff + refresh automático.
Todos os endpoints confirmados em `api_clubdacotacao_instrucoes.md`.
"""
from __future__ import annotations

import asyncio
import re
import time
from datetime import date
from decimal import Decimal
from typing import Any

import httpx

# Regex para extrair placa brasileira do texto de observação da cotação.
# Formato Mercosul: AAA0A00 ou antigo: AAA0000 (com ou sem hífen)
_RE_PLACA = re.compile(r"\b([A-Z]{3})-?(\d[A-Z0-9]\d{2})\b", re.IGNORECASE)
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


class ClubNotFoundError(Exception):
    """404 do Club — recurso nao existe. Tratada como erro do cliente
    (id invalido), NAO como sinal de API fora do ar. Por isso eh
    ignorada pelo circuit breaker."""


# IDs de pedido no Club sao inteiros — usado para rejeitar entradas
# nao-numericas (ex: codigo_oc = "MAGNA-001" vindo do Pipefy) antes
# de fazer a chamada, evitando 404s que consumiriam o breaker.
_RE_ID_PEDIDO = re.compile(r"^\d+$")


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
        self._breaker = CircuitBreaker(
            "club",
            fail_threshold=5,
            reset_timeout=60,
            # 404 nao indica Club fora do ar — nao conta pro threshold
            ignored_excs=(ClubNotFoundError,),
        )
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

                if resp.status_code == 404:
                    # Recurso nao existe — erro do cliente, nao do Club.
                    # Usa exception propria pra ser ignorada pelo breaker.
                    raise ClubNotFoundError(
                        f"404 em {url}: {resp.text[:200]}"
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
        # Normalizar placa: v1 retorna identificador=null, mas a placa pode
        # estar embutida em observacao / cot_obs / request.obs (dependendo
        # do tipo de cotacao). Para OCs ML o payload nao contem placa e a
        # busca retorna None — e limitacao arquitetural documentada.
        return [self._normalizar_pedido_v1(p) for p in (pedidos or [])]

    @staticmethod
    def _normalizar_pedido_v1(raw: dict[str, Any]) -> dict[str, Any]:
        """Extrai a placa de campos de observacao quando identificador
        vem null (acontece na listagem v1 para a maioria das cotacoes).

        Ordem de prioridade dos campos de origem:
          1. identificador / identifier (se ja presentes)
          2. request.obs (mesmo campo usado por _normalizar_pedido_v3)
          3. observacao (raiz v1)
          4. cot_obs (raiz v1)

        OCs MERCADO LIVRE nao contem placa em nenhum campo do payload v1
        (observacao traz JSON do ML sem placa). Para estas, identificador
        permanece None e aparecera como "--" no dashboard; o analista ve
        o fornecedor MERCADO LIVRE e sabe que precisa verificar manualmente.
        """
        if raw.get("identificador") or raw.get("identifier"):
            return raw
        candidatos: list[str] = []
        req = raw.get("request")
        if isinstance(req, dict) and isinstance(req.get("obs"), str):
            candidatos.append(req["obs"])
        obs = raw.get("observacao")
        if isinstance(obs, str):
            candidatos.append(obs)
        cot_obs = raw.get("cot_obs")
        if isinstance(cot_obs, str):
            candidatos.append(cot_obs)
        for texto in candidatos:
            if not texto:
                continue
            m = _RE_PLACA.search(texto)
            if m:
                placa = f"{m.group(1).upper()}-{m.group(2).upper()}"
                raw["identificador"] = placa
                raw["identifier"] = placa
                break
        return raw

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
        # A API v3 NÃO retorna 'identifier' diretamente no pedido.
        # A placa está embutida no campo request.obs da cotação, ex:
        # "PRISMA (2017) QQF-2C69 — Cinza — 9BGK..."
        if "identificador" not in raw and "identifier" in raw:
            raw["identificador"] = raw["identifier"]
        if not raw.get("identificador") and not raw.get("identifier"):
            req = raw.get("request") or {}
            obs = req.get("obs") or ""
            if obs:
                m = _RE_PLACA.search(obs)
                if m:
                    raw["identificador"] = f"{m.group(1).upper()}-{m.group(2).upper()}"
                    raw["identifier"] = raw["identificador"]

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

    async def listar_ofertas_por_peca(
        self, id_cotacao: str | int, *, page_size: int = 100
    ) -> dict[str, int]:
        """GET /api/v2/requests/{id}/products/offers?selected_only=0

        Retorna o numero de fornecedores que ofertaram cada peca dentro
        da cotacao (independente de terem sido selecionados ou nao).
        Usado por R1 para validar o minimo de 3 cotacoes POR PECA
        (nao pelo total global da cotacao).

        Retorno: dict { produto_id (str): qtd_fornecedores }
        Se falhar, retorna {} — R1 cai no fallback por contagem global.
        """
        url = f"{self._base_v1}/v2/requests/{id_cotacao}/products/offers"
        params = {
            "product_filter": "",
            "page": "1",
            "page_size": str(page_size),
            "fornecedor": "",
            "imprimir": "false",
            "obs_only": "false",
            "others_only": "false",
            "variation_only": "false",
            "category_id": "",
            # selected_only=0 traz TODOS os que ofertaram (nao so vencedores)
            "selected_only": "0",
        }
        try:
            data = await self._request("GET", url, params=params)
        except Exception as e:
            logger.warning(
                "Falha ao listar ofertas por peca da cotacao %s: %s",
                id_cotacao, e,
            )
            return {}
        produtos = data.get("produtos") if isinstance(data, dict) else data
        if not isinstance(produtos, list):
            return {}
        resultado: dict[str, int] = {}
        for p in produtos:
            if not isinstance(p, dict):
                continue
            # O endpoint chama a lista de fornecedores que ofertaram de
            # "vencedores" (mesmo com selected_only=0 traz todos).
            vencedores = p.get("vencedores") or []
            prod_id = p.get("prod_id") or p.get("pro_id") or p.get("produto_id")
            if prod_id is not None:
                resultado[str(prod_id)] = len(vencedores)
        return resultado

    async def get_order_details(self, id_pedido: str | int) -> dict[str, Any]:
        """GET /v3/api/clients/orders/{id} — detalhes completos com items[].

        Valida que `id_pedido` eh numerico ANTES de chamar a rede. Cards
        do Pipefy com `codigo_oc` invalido (ex: "MAGNA-001") chegavam ate
        aqui, geravam 404 e consumiam o circuit breaker — bloqueando
        chamadas legitimas subsequentes. Com a validacao, esses casos
        levantam ValueError que eh tratado pelos callers sem tocar rede
        nem breaker.
        """
        s = str(id_pedido).strip()
        if not _RE_ID_PEDIDO.match(s):
            raise ValueError(
                f"id_pedido invalido (esperado numero inteiro): {id_pedido!r}"
            )
        url = f"{self._base_v3}/clients/orders/{s}"
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

    async def listar_produtos_por_placa(
        self,
        placa: str,
        data_inicio: date,
        data_fim: date,
    ) -> list[dict[str, Any]]:
        """GET /api/getprodutosrelatoriocliente?identifier={placa}&dateIni&dateFim

        Retorna TODOS os produtos comprados para a placa no periodo,
        com a OC (id_pedido), cotacao (id_cotacao), data, fornecedor e valores.
        Eh o mesmo endpoint usado pelo relatorio Club > Produtos por placa.

        Fonte autoritativa para a R2 cross-time: cobre OCs antigas em que
        request.obs nao tem a placa indexada (caso em que listar_pedidos_v3
        nao consegue mapear a OC para a placa).

        Importante: NAO enviar `groupBy=pe.id_pedido,pe.id_vendedor` (filtro
        usado pela tela de impressao colapsa duplicatas e oculta pecas
        recompradas em OCs distintas — exatamente o que o R2 precisa ver).

        Limitacao do Club: o endpoint rejeita janelas maiores que 6 meses
        (HTTP 422). Janelas maiores sao quebradas em chunks de ~180 dias
        e os resultados sao concatenados (com dedupe por
        (id_pedido, ean ou cod_interno ou produto_id ou indice)).

        Defense-in-depth: a placa eh validada contra `_RE_PLACA` antes do
        request — protege contra chamada com identifier vazio/invalido
        que poderia retornar dados de outras placas.
        """
        from datetime import timedelta as _td

        if not _RE_PLACA.fullmatch(placa or ""):
            raise ValueError(f"placa invalida para listar_produtos_por_placa")
        if data_fim < data_inicio:
            return []

        MAX_DIAS = 180  # 6 meses (limite duro do Club)
        url = f"{self._base_v1}/getprodutosrelatoriocliente"

        # Quebrar em chunks se necessario
        chunks: list[tuple[date, date]] = []
        cursor_fim = data_fim
        while cursor_fim >= data_inicio:
            cursor_ini = max(data_inicio, cursor_fim - _td(days=MAX_DIAS - 1))
            chunks.append((cursor_ini, cursor_fim))
            if cursor_ini == data_inicio:
                break
            cursor_fim = cursor_ini - _td(days=1)

        todos: list[dict[str, Any]] = []
        vistos: set[tuple[str, str]] = set()
        for ini, fim in chunks:
            params: dict[str, Any] = {
                "identifier": placa,
                "dateIni": ini.strftime("%Y-%m-%d"),
                "dateFim": fim.strftime("%Y-%m-%d"),
                "ordenar": "data_geracao",
                "tipoorder": "desc",
                "imprimir": "true",
            }
            data = await self._request("GET", url, params=params)
            produtos: list[dict[str, Any]] = []
            if isinstance(data, dict):
                produtos = data.get("produtos") or []
                if not isinstance(produtos, list):
                    produtos = []
            elif isinstance(data, list):
                produtos = data
            for idx, p in enumerate(produtos):
                # Dedupe key com fallback em cascata para evitar colapso
                # quando ean vier vazio em multiplos items do mesmo pedido.
                ident_peca = (
                    str(p.get("ean") or "").strip()
                    or str(p.get("pro_ean") or "").strip()
                    or str(p.get("cod_interno") or "").strip()
                    or str(p.get("produto_id") or "").strip()
                    or str(p.get("pro_id") or "").strip()
                    or f"idx:{len(todos) + idx}"
                )
                chave = (str(p.get("id_pedido") or "").strip(), ident_peca)
                if chave in vistos:
                    continue
                vistos.add(chave)
                todos.append(p)

        return todos

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
