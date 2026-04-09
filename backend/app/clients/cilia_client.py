"""Cliente Cilia — interface + 4 implementações:

  - CiliaStub:        dados sintéticos determinísticos (desenvolvimento)
  - CiliaHTTPClient:  cliente HTTP real com login automático via cookie Rails
  - CiliaDeeplinkClient: não chama Cilia, só fornece link clicável
  - CiliaOff:         desativa completamente

Troca via `CILIA_MODE`: stub | http | deeplink | off (default: stub).

A implementação HTTP usa o padrão do `ClubClient` (httpx.AsyncClient
persistente, retry com tenacity, audit, rate limit defensivo) e mantém
o cookie de sessão `_cilia_session` em arquivo (`data/cilia_session.json`)
para evitar relogin a cada execução.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
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
from app.models import ItemCilia, OrcamentoCilia

logger = get_logger(__name__)


class CiliaError(Exception):
    """Erro genérico do cliente Cilia."""


class CiliaAuthError(CiliaError):
    """Falha de autenticação ou sessão expirada (login inválido, reCAPTCHA, etc)."""


class CiliaClient(ABC):
    """Interface que qualquer implementação do Cilia deve respeitar."""

    @abstractmethod
    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        """Retorna o orçamento mais recente do Cilia para a placa dada."""

    async def close(self) -> None:
        pass


# ----------------------------------------------------------------------
# Stub — gera dados determinísticos por placa para desenvolvimento
# ----------------------------------------------------------------------

class CiliaStub(CiliaClient):
    """Implementação fake: gera orçamento determinístico por placa.

    Usado enquanto as credenciais reais do Cilia não estão disponíveis.
    A mesma placa sempre retorna os mesmos valores (hash determinístico),
    o que permite escrever testes reproduzíveis contra o stub.
    """

    def __init__(self) -> None:
        self._cache: dict[str, OrcamentoCilia] = {}

    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        placa_norm = placa.replace("-", "").upper().strip()
        if not placa_norm:
            return None

        if placa_norm in self._cache:
            return self._cache[placa_norm]

        # Placas terminadas em "99" → simular "não encontrado"
        if placa_norm.endswith("99"):
            logger.debug("Cilia stub: placa %s não encontrada (simulado)", placa_norm)
            return None

        seed = int(hashlib.md5(placa_norm.encode()).hexdigest(), 16)

        # Valor base entre R$ 500 e R$ 5000 (determinístico por placa)
        valor_total = Decimal(f"{500 + (seed % 4500)}.{seed % 100:02d}")
        qtd_itens = 1 + (seed % 4)  # 1 a 4 itens

        itens = []
        descricoes = [
            "Para-choque dianteiro",
            "Farol esquerdo",
            "Capô",
            "Paralama dianteiro direito",
            "Retrovisor lateral",
            "Porta dianteira",
            "Para-lama traseiro",
            "Tampa traseira",
        ]
        valor_restante = valor_total
        for i in range(qtd_itens):
            if i == qtd_itens - 1:
                vi = valor_restante
            else:
                vi = (valor_total / qtd_itens).quantize(Decimal("0.01"))
                valor_restante -= vi
            itens.append(
                ItemCilia(
                    descricao=descricoes[(seed + i) % len(descricoes)],
                    quantidade=1,
                    valor_unitario=vi,
                    valor_total=vi,
                )
            )

        orc = OrcamentoCilia(
            placa=placa_norm,
            numero_orcamento=f"CIL-{seed % 100000:05d}",
            data=date.today() - timedelta(days=1),
            valor_total=valor_total,
            itens=itens,
            encontrado=True,
        )
        self._cache[placa_norm] = orc
        return orc


# ----------------------------------------------------------------------
# Cliente HTTP real (cookie Rails + login automático + cache de sessão)
# ----------------------------------------------------------------------

# Regex para extrair CSRF token do HTML do form de login
_RE_CSRF_META = re.compile(
    r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_RE_CSRF_INPUT = re.compile(
    r'<input[^>]*name=["\']authenticity_token["\'][^>]*value=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
# Padrão de placa: 3 letras + 4 dígitos OU formato Mercosul (3 letras + dígito + letra + 2 dígitos)
_RE_DATE_BR = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _to_date_br(d: date) -> str:
    """Converte date Python para o formato DD/MM/AAAA usado pelo Cilia."""
    return d.strftime("%d/%m/%Y")


class CiliaHTTPClient(CiliaClient):
    """Cliente HTTP real para Cilia.

    Estratégia:
      1. Sessão `httpx.AsyncClient` persistente com cookies habilitados
      2. Login automático via POST /users/sign_in (form-urlencoded + CSRF)
      3. Cookie `_cilia_session` é persistido em arquivo para reutilização
         entre execuções (idade < 23h)
      4. Detecção de sessão expirada (302/HTML/401) → relogin + retry
      5. Rate limit defensivo (`cilia_request_delay_ms` entre requests)
      6. Auditoria via `registrar_chamada_api("cilia", ...)`

    O método público é `consultar_por_placa(placa)` que faz 2 chamadas:
      - GET /api/surveys/search.json (busca por placa + range de datas)
      - GET /api/surveys/{id}/preview (detalhes do orçamento mais recente)
    """

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(
        self,
        base_url: str | None = None,
        login: str | None = None,
        senha: str | None = None,
        session_file: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = (base_url or settings.cilia_base_url).rstrip("/")
        self._login = login or settings.cilia_login
        self._senha = senha or settings.cilia_senha
        self._session_file = Path(
            session_file or settings.cilia_session_file
        )
        # Caminho relativo → relativo a backend/
        if not self._session_file.is_absolute():
            from app.config import BASE_DIR
            self._session_file = BASE_DIR / self._session_file

        if not self._login or not self._senha:
            raise CiliaError(
                "CILIA_LOGIN/CILIA_SENHA não configurados — "
                "use CILIA_MODE=stub até preencher credenciais"
            )

        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "User-Agent": self.USER_AGENT,
                "Accept": "application/json, text/html",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
            },
            follow_redirects=False,  # precisamos detectar 302 do login
        )
        self._last_request_at = 0.0
        self._authenticated = False
        # Tenta carregar sessão persistida no startup
        self._restaurar_sessao()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "CiliaHTTPClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Sessão persistida em arquivo
    # ------------------------------------------------------------------
    def _restaurar_sessao(self) -> None:
        """Carrega cookie do arquivo se existir e ainda for fresco (<23h)."""
        try:
            if not self._session_file.exists():
                return
            data = json.loads(self._session_file.read_text(encoding="utf-8"))
            ts = datetime.fromisoformat(data["timestamp"])
            idade_h = (datetime.now() - ts).total_seconds() / 3600
            if idade_h > 23:
                logger.info(
                    "Cilia: sessão em cache expirou (%.1fh) — vai relogar",
                    idade_h,
                )
                return
            for cookie in data.get("cookies", []):
                self._client.cookies.set(
                    cookie["name"], cookie["value"],
                    domain=cookie.get("domain"),
                    path=cookie.get("path", "/"),
                )
            self._authenticated = True
            logger.info("Cilia: sessão restaurada do cache (idade %.1fh)", idade_h)
        except Exception as e:
            logger.warning("Cilia: falha ao restaurar sessão: %s", e)

    def _persistir_sessao(self) -> None:
        """Salva cookies atuais em arquivo (idade=now)."""
        try:
            self._session_file.parent.mkdir(parents=True, exist_ok=True)
            cookies = []
            for c in self._client.cookies.jar:
                cookies.append({
                    "name": c.name,
                    "value": c.value,
                    "domain": c.domain,
                    "path": c.path,
                })
            data = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "cookies": cookies,
            }
            self._session_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.debug("Cilia: sessão persistida em %s", self._session_file)
        except Exception as e:
            logger.warning("Cilia: falha ao persistir sessão: %s", e)

    # ------------------------------------------------------------------
    # Throttle defensivo
    # ------------------------------------------------------------------
    async def _throttle(self) -> None:
        """Garante delay mínimo entre requests."""
        delay_s = settings.cilia_request_delay_ms / 1000.0
        agora = time.perf_counter()
        elapsed = agora - self._last_request_at
        if elapsed < delay_s:
            await asyncio.sleep(delay_s - elapsed)
        self._last_request_at = time.perf_counter()

    # ------------------------------------------------------------------
    # Auditoria
    # ------------------------------------------------------------------
    def _audit(
        self,
        method: str,
        url: str,
        status: int | None,
        duracao_ms: int,
        erro: str | None = None,
    ) -> None:
        try:
            registrar_chamada_api(
                "cilia", method, url, status, duracao_ms, erro
            )
        except Exception as e:
            logger.debug("Falha ao registrar audit Cilia: %s", e)

    # ------------------------------------------------------------------
    # CSRF + login
    # ------------------------------------------------------------------
    async def _obter_csrf_token(self) -> str:
        """GET /users/sign_in → extrai CSRF token do HTML.

        O cookie da sessão pré-login é mantido automaticamente no
        cookie jar do `httpx.AsyncClient`.
        """
        url = f"{self._base_url}/users/sign_in"
        await self._throttle()
        started = time.perf_counter()
        try:
            # Para o GET inicial precisamos aceitar HTML (não só JSON)
            resp = await self._client.get(
                url,
                headers={"Accept": "text/html,application/xhtml+xml"},
            )
        except httpx.RequestError as e:
            self._audit(
                "GET", url, None,
                int((time.perf_counter() - started) * 1000), str(e),
            )
            raise CiliaError(f"Falha de rede no GET sign_in: {e}") from e

        duracao = int((time.perf_counter() - started) * 1000)
        self._audit("GET", url, resp.status_code, duracao)

        if resp.status_code != 200:
            raise CiliaAuthError(
                f"GET /users/sign_in retornou HTTP {resp.status_code}"
            )

        # Tenta extrair CSRF — primeiro <meta>, depois <input>
        m = _RE_CSRF_META.search(resp.text)
        if not m:
            m = _RE_CSRF_INPUT.search(resp.text)
        if not m:
            raise CiliaAuthError(
                "CSRF token não encontrado no HTML do form de login"
            )
        return m.group(1)

    async def _autenticar(self) -> None:
        """Faz login completo: GET sign_in (csrf) → POST sign_in.

        Define `self._authenticated = True` em sucesso e persiste o
        cookie em arquivo para próximas execuções.
        """
        logger.info("Cilia: autenticando como %s", self._login)
        csrf = await self._obter_csrf_token()

        url = f"{self._base_url}/users/sign_in"
        form = {
            "user[email]": self._login,
            "user[password]": self._senha,
            "authenticity_token": csrf,
        }
        await self._throttle()
        started = time.perf_counter()
        try:
            resp = await self._client.post(
                url,
                data=form,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "text/html,application/xhtml+xml",
                    "Referer": url,
                    "Origin": self._base_url,
                },
            )
        except httpx.RequestError as e:
            self._audit(
                "POST", url, None,
                int((time.perf_counter() - started) * 1000), str(e),
            )
            raise CiliaError(f"Falha de rede no POST sign_in: {e}") from e

        duracao = int((time.perf_counter() - started) * 1000)
        self._audit("POST", url, resp.status_code, duracao)

        # Sucesso típico Rails: HTTP 302 + Set-Cookie _cilia_session
        if resp.status_code != 302:
            # Pode ser 200 com HTML de erro (credencial inválida) ou
            # 422 (CSRF inválido / reCAPTCHA)
            if resp.status_code == 200 and "sign_in" in resp.text.lower():
                raise CiliaAuthError(
                    "Login retornou HTML do form (credencial inválida ou reCAPTCHA)"
                )
            raise CiliaAuthError(
                f"POST /users/sign_in retornou HTTP {resp.status_code}"
            )

        # Verifica que recebeu o cookie de sessão
        cookies_recebidos = [c.name for c in self._client.cookies.jar]
        if not any("session" in c.lower() for c in cookies_recebidos):
            raise CiliaAuthError(
                "Login 302 mas Set-Cookie de sessão não foi retornado"
            )

        # Validação extra: pinga /api/users/current_user
        if not await self._validar_sessao():
            raise CiliaAuthError(
                "Login parece ter funcionado mas /current_user não retornou 200"
            )

        self._authenticated = True
        self._persistir_sessao()
        logger.info("Cilia: autenticado com sucesso")

    async def _validar_sessao(self) -> bool:
        """Verifica se a sessão atual é válida via GET /api/users/current_user.

        Retorna True se HTTP 200 + JSON com chave 'id', False caso contrário.
        Não lança exceção — usado para detecção silenciosa.
        """
        url = f"{self._base_url}/api/users/current_user"
        await self._throttle()
        started = time.perf_counter()
        try:
            resp = await self._client.get(
                url,
                headers={"Accept": "application/json"},
            )
        except httpx.RequestError as e:
            self._audit(
                "GET", url, None,
                int((time.perf_counter() - started) * 1000), str(e),
            )
            return False
        duracao = int((time.perf_counter() - started) * 1000)
        self._audit("GET", url, resp.status_code, duracao)
        if resp.status_code != 200:
            return False
        try:
            data = resp.json()
            return bool(data.get("id") or data.get("user_id"))
        except Exception:
            return False

    async def _garantir_autenticado(self) -> None:
        """Se não está autenticado, faz login. Idempotente."""
        if not self._authenticated:
            await self._autenticar()

    # ------------------------------------------------------------------
    # Wrapper de request com retry e detecção de sessão expirada
    # ------------------------------------------------------------------
    async def _request(
        self, method: str, url: str, **kwargs: Any
    ) -> httpx.Response:
        """Faz uma chamada HTTP autenticada com retry + audit.

        Detecta sessão expirada (302/HTML/401) e refaz o login uma vez.
        """
        await self._garantir_autenticado()

        async def _do_request() -> httpx.Response:
            await self._throttle()
            started = time.perf_counter()
            try:
                resp = await self._client.request(method, url, **kwargs)
            except httpx.RequestError as e:
                self._audit(
                    method, url, None,
                    int((time.perf_counter() - started) * 1000), str(e),
                )
                raise CiliaError(f"Erro de rede em {method} {url}: {e}") from e
            duracao = int((time.perf_counter() - started) * 1000)
            self._audit(method, url, resp.status_code, duracao)
            return resp

        # 1ª tentativa
        resp = await _do_request()

        # Detecção de sessão expirada
        sessao_expirou = (
            resp.status_code in (302, 401)
            or (
                resp.status_code == 200
                and "text/html" in resp.headers.get("content-type", "").lower()
                and "sign_in" in resp.text[:500].lower()
            )
        )
        if sessao_expirou:
            logger.warning(
                "Cilia: sessão expirou em %s (status=%d) — relogando",
                url, resp.status_code,
            )
            self._authenticated = False
            try:
                self._session_file.unlink(missing_ok=True)
            except Exception:
                pass
            await self._autenticar()
            resp = await _do_request()

        if resp.status_code >= 400:
            raise CiliaError(
                f"HTTP {resp.status_code} em {method} {url}: {resp.text[:200]}"
            )

        return resp

    # ------------------------------------------------------------------
    # Método público: consultar orçamento por placa
    # ------------------------------------------------------------------
    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        """Busca o orçamento mais recente do Cilia para a placa dada.

        Faz 2 chamadas em sequência:
          1. GET /api/surveys/search.json → array de surveys
          2. GET /api/surveys/{id}/preview → detalhes + items
        """
        if not placa:
            return None

        # Cilia espera a placa COM hífen (ex: "PQX-2I72"). Se vier
        # normalizada (sem hífen), reinserimos:
        placa_busca = self._formatar_placa_com_hifen(placa)
        placa_norm = placa.replace("-", "").upper().strip()

        # 1. Search
        hoje = date.today()
        inicio = hoje - timedelta(days=settings.cilia_search_janela_dias)
        params = {
            "search_filters[license_plate]": placa_busca,
            "search_filters[date_type]": "creation",
            "search_filters[date_range][start_date]": _to_date_br(inicio),
            "search_filters[date_range][end_date]": _to_date_br(hoje),
            "page": 1,
        }
        url = f"{self._base_url}/api/surveys/search.json"
        try:
            resp = await self._request("GET", url, params=params)
            data = resp.json()
        except Exception as e:
            logger.warning("Cilia search falhou para %s: %s", placa, e)
            return None

        # A resposta pode vir como { "surveys": [...] } ou variantes
        surveys = (
            data.get("surveys")
            or data.get("data", {}).get("surveys")
            or data.get("results")
            or []
        )
        if not surveys and isinstance(data, list):
            surveys = data
        if not surveys:
            logger.debug("Cilia: nenhum survey para placa %s", placa)
            return OrcamentoCilia(placa=placa_norm, encontrado=False)

        # Pega o mais recente (primeiro da lista, ou ordena por created_at)
        try:
            surveys_sorted = sorted(
                surveys,
                key=lambda s: s.get("created_at") or s.get("creation_date") or "",
                reverse=True,
            )
        except Exception:
            surveys_sorted = surveys
        survey = surveys_sorted[0]
        survey_id = survey.get("id") or survey.get("survey_id")
        if not survey_id:
            logger.debug("Cilia: survey sem id para %s", placa)
            return OrcamentoCilia(placa=placa_norm, encontrado=False)

        # 2. Preview
        url_prev = f"{self._base_url}/api/surveys/{survey_id}/preview"
        try:
            resp = await self._request(
                "GET", url_prev,
                params={"budget_type": "InsurerBudget"},
            )
            preview = resp.json()
        except Exception as e:
            logger.warning(
                "Cilia preview survey %s falhou: %s", survey_id, e
            )
            return OrcamentoCilia(placa=placa_norm, encontrado=False)

        # Parser defensivo: o schema exato não é documentado.
        # Tentamos várias chaves comuns.
        return self._parse_preview(preview, placa_norm, survey_id)

    @staticmethod
    def _formatar_placa_com_hifen(placa: str) -> str:
        """Garante que a placa esteja no formato AAA-9999 ou AAA-9A99."""
        s = placa.replace("-", "").replace(" ", "").upper().strip()
        if len(s) == 7 and "-" not in placa:
            return f"{s[:3]}-{s[3:]}"
        return placa.upper().strip()

    def _parse_preview(
        self, preview: dict, placa_norm: str, survey_id: Any
    ) -> OrcamentoCilia:
        """Parser defensivo do JSON do /api/surveys/{id}/preview.

        Tenta várias chaves comuns para encontrar a lista de items e
        cair graciosamente se o schema for diferente do esperado.
        """
        # Estrutura comum Rails: { "budget": {...}, "survey": {...}, "items": [...] }
        budget = (
            preview.get("budget")
            or preview.get("data", {}).get("budget")
            or preview
        )
        if not isinstance(budget, dict):
            budget = preview

        items_raw = (
            budget.get("items")
            or budget.get("budget_items")
            or budget.get("pieces")
            or preview.get("items")
            or []
        )

        itens: list[ItemCilia] = []
        for it in items_raw if isinstance(items_raw, list) else []:
            try:
                desc = (
                    it.get("description")
                    or it.get("descricao")
                    or it.get("name")
                    or it.get("piece_name")
                    or ""
                )
                qty_raw = (
                    it.get("quantity")
                    or it.get("quantidade")
                    or it.get("qty")
                    or 1
                )
                qty = float(qty_raw or 0)
                vu = it.get("unit_price") or it.get("valor_unitario")
                vt = it.get("total_price") or it.get("valor_total")
                itens.append(
                    ItemCilia(
                        descricao=str(desc).strip() or "(sem descrição)",
                        quantidade=qty,
                        valor_unitario=Decimal(str(vu)) if vu is not None else None,
                        valor_total=Decimal(str(vt)) if vt is not None else None,
                    )
                )
            except Exception as e:
                logger.debug("Cilia: item ignorado por erro de parse: %s", e)
                continue

        valor_total_raw = (
            budget.get("total_price")
            or budget.get("valor_total")
            or budget.get("total")
        )
        valor_total = None
        if valor_total_raw is not None:
            try:
                valor_total = Decimal(str(valor_total_raw))
            except Exception:
                pass

        # Data do orçamento
        data_str = (
            budget.get("created_at")
            or budget.get("creation_date")
            or budget.get("data")
        )
        data_obj: date | None = None
        if data_str:
            try:
                data_obj = datetime.fromisoformat(
                    str(data_str).replace("Z", "+00:00")
                ).date()
            except Exception:
                pass

        numero = (
            budget.get("budget_number")
            or budget.get("numero_orcamento")
            or str(survey_id)
        )

        return OrcamentoCilia(
            placa=placa_norm,
            numero_orcamento=str(numero) if numero else None,
            data=data_obj,
            valor_total=valor_total,
            itens=itens,
            encontrado=bool(itens),
        )


# ----------------------------------------------------------------------
# Deeplink — não chama Cilia, só renderiza link clicável
# ----------------------------------------------------------------------

class CiliaDeeplinkClient(CiliaClient):
    """Modo deeplink: NÃO faz HTTP, apenas retorna `OrcamentoCilia` vazio
    sinalizando que o relatório deve renderizar um link para o usuário
    abrir manualmente o Cilia naquela placa.

    Útil quando reCAPTCHA bloqueia o login automático mas queremos pelo
    menos facilitar a navegação manual do analista.
    """

    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        placa_norm = placa.replace("-", "").upper().strip()
        if not placa_norm:
            return None
        return OrcamentoCilia(placa=placa_norm, encontrado=False, itens=[])


# ----------------------------------------------------------------------
# Off — desativa Cilia
# ----------------------------------------------------------------------

class CiliaOff(CiliaClient):
    """Modo off: nunca consulta nada. Retorna sempre None."""

    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        return None


# ----------------------------------------------------------------------
# Factory
# ----------------------------------------------------------------------

def build_cilia_client() -> CiliaClient:
    """Escolhe a implementação conforme settings.cilia_mode."""
    mode = (settings.cilia_mode or "stub").lower()
    if mode == "stub":
        logger.info("Cilia: usando CiliaStub (modo desenvolvimento)")
        return CiliaStub()
    if mode == "http":
        try:
            client = CiliaHTTPClient()
            logger.info(
                "Cilia: usando CiliaHTTPClient (login=%s, base=%s)",
                settings.cilia_login or "<não configurado>",
                settings.cilia_base_url,
            )
            return client
        except CiliaError as e:
            logger.error(
                "Cilia: falha ao instanciar HTTP — caindo para stub: %s", e
            )
            return CiliaStub()
    if mode == "deeplink":
        logger.info("Cilia: usando CiliaDeeplinkClient (link manual)")
        return CiliaDeeplinkClient()
    if mode == "off":
        logger.info("Cilia: desativado (CILIA_MODE=off)")
        return CiliaOff()
    raise RuntimeError(
        f"CILIA_MODE inválido: {mode!r} (use stub|http|deeplink|off)"
    )
