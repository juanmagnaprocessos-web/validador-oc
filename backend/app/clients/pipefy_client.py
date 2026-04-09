"""Cliente GraphQL para Pipefy.

Docs: https://developers.pipefy.com/
Endpoint único: https://api.pipefy.com/graphql
Autenticação: Bearer token (obtido em https://app.pipefy.com/tokens)

Usa os IDs de fases/campos descobertos por `scripts/descobrir_ids_pipefy.py`
e armazenados em `config/pipefy_ids.json`.
"""
from __future__ import annotations

import json
import time
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def _norm_label(s: str) -> str:
    """Lowercase + remove acentos para comparação robusta de labels."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

import httpx

from app.clients.pdf_parser import extrair_valor_total, extrair_valor_total_async
from app.config import settings
from app.db import registrar_chamada_api
from app.logging_setup import get_logger
from app.models import CardPipefy
from app.utils.circuit_breaker import CircuitBreaker

logger = get_logger(__name__)


class PipefyError(Exception):
    pass


# ---------- carregamento do mapeamento de IDs ----------

class PipefyIds:
    """Encapsula o config/pipefy_ids.json gerado pelo script introspectivo."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data
        self.pipe_id: int = data["pipe_id"]
        self.phases: dict[str, Any] = data.get("phases", {})
        self.fase_destino: dict[str, str | None] = data.get("fase_destino", {})
        self.campos_validacao: dict[str, dict | None] = data.get("campos_validacao", {})

    @classmethod
    def load(cls, path: Path | None = None) -> "PipefyIds":
        p = path or settings.pipefy_ids_full_path
        if not p.exists():
            raise PipefyError(
                f"{p} não existe. Rode primeiro: python -m scripts.descobrir_ids_pipefy"
            )
        return cls(json.loads(p.read_text(encoding="utf-8")))

    def fase_id(self, chave: str) -> str:
        v = self.fase_destino.get(chave)
        if not v:
            raise PipefyError(f"Fase '{chave}' não mapeada em pipefy_ids.json")
        return v

    def campo_id(self, chave: str) -> str:
        v = self.campos_validacao.get(chave)
        if not v or not v.get("id"):
            raise PipefyError(f"Campo '{chave}' não mapeado em pipefy_ids.json")
        return v["id"]


# ---------- cliente HTTP ----------

class PipefyClient:
    def __init__(
        self,
        token: str | None = None,
        ids: PipefyIds | None = None,
        *,
        dry_run: bool = True,
        timeout: float = 30.0,
    ) -> None:
        self._token = token or settings.pipefy_token
        if not self._token or "SUBSTITUIR" in self._token:
            raise PipefyError("PIPEFY_TOKEN não configurado")
        self._url = settings.pipefy_api_url
        self.ids = ids  # lazy load se None
        self.dry_run = dry_run
        self._breaker = CircuitBreaker("pipefy", fail_threshold=5, reset_timeout=60)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
        )

    async def __aenter__(self) -> "PipefyClient":
        if self.ids is None:
            self.ids = PipefyIds.load()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        await self._client.aclose()

    def _ids(self) -> PipefyIds:
        if self.ids is None:
            self.ids = PipefyIds.load()
        return self.ids

    # ---------- chamada GraphQL base ----------

    async def _gql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Chamada GraphQL protegida por circuit breaker."""
        return await self._breaker.call(
            self._do_gql, query, variables
        )

    async def _do_gql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            resp = await self._client.post(
                self._url, json={"query": query, "variables": variables or {}}
            )
        except httpx.RequestError as e:
            registrar_chamada_api(
                "pipefy", "POST", self._url, None,
                int((time.perf_counter() - started) * 1000), str(e),
            )
            raise PipefyError(f"Erro de rede: {e}") from e

        duracao = int((time.perf_counter() - started) * 1000)
        registrar_chamada_api("pipefy", "POST", self._url, resp.status_code, duracao)

        if resp.status_code != 200:
            raise PipefyError(f"HTTP {resp.status_code}: {resp.text[:500]}")

        payload = resp.json()
        if "errors" in payload:
            raise PipefyError(f"GraphQL errors: {payload['errors']}")

        return payload.get("data", {})

    # ---------- leitura ----------

    LIST_CARDS_QUERY = """
    query($phaseId: ID!, $first: Int!, $after: String) {
      phase(id: $phaseId) {
        cards(first: $first, after: $after) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              title
              created_at
              current_phase { id name }
              fields {
                name
                value
                field { id label type internal_id }
                array_value
              }
              attachments {
                url
                path
              }
            }
          }
        }
      }
    }
    """

    async def listar_cards_fase(
        self, fase_chave: str = "validacao", *, max_cards: int = 500
    ) -> list[CardPipefy]:
        """Lista todos os cards da fase de validação."""
        phase_id = self._ids().fase_id(fase_chave)
        cards: list[CardPipefy] = []
        after: str | None = None

        while True:
            data = await self._gql(
                self.LIST_CARDS_QUERY,
                {"phaseId": phase_id, "first": 50, "after": after},
            )
            phase = data.get("phase") or {}
            conn = phase.get("cards") or {}
            for edge in conn.get("edges", []):
                cards.append(self._parse_card(edge["node"]))
            page_info = conn.get("pageInfo") or {}
            if not page_info.get("hasNextPage") or len(cards) >= max_cards:
                break
            after = page_info.get("endCursor")

        logger.info(
            "Pipefy: %d cards encontrados em fase %s", len(cards), fase_chave
        )
        return cards

    GET_CARD_QUERY = """
    query($id: ID!) {
      card(id: $id) {
        id
        title
        created_at
        current_phase { id name }
        fields {
          name
          value
          field { id label type internal_id }
          array_value
        }
        attachments { url path }
      }
    }
    """

    async def get_card(self, card_id: str) -> CardPipefy:
        data = await self._gql(self.GET_CARD_QUERY, {"id": card_id})
        node = data.get("card")
        if not node:
            raise PipefyError(f"Card {card_id} não encontrado")
        return self._parse_card(node)

    def _parse_card(self, node: dict[str, Any]) -> CardPipefy:
        ids = self._ids()
        campo_anexo = (ids.campos_validacao.get("ordem_de_compra_pdf") or {}).get("id")
        campo_codigo = (ids.campos_validacao.get("codigo_oc") or {}).get("id")

        campos: dict[str, Any] = {}
        codigo_oc: str | None = None
        anexo_url: str | None = None
        anexo_cilia_url: str | None = None
        valor_card: Decimal | None = None
        descricao_pecas: str | None = None
        forma_pagamento: str | None = None
        origem_peca: str | None = None

        for f in node.get("fields") or []:
            field_def = f.get("field") or {}
            fid = field_def.get("id")
            label = field_def.get("label") or ""
            ftype = field_def.get("type")
            val = f.get("value")
            campos[label] = val

            if fid and fid == campo_codigo and val:
                codigo_oc = str(val)
            if fid and fid == campo_anexo and val:
                anexo_url = _primeira_url(val) or _primeira_url(f.get("array_value"))

            # Forma de pagamento e Origem da peça — campos do start form
            # do card (radio_vertical / select). São a fonte canônica para
            # decidir a fase de destino e detectar Mercado Livre.
            if fid == "forma_de_pagamento" and val:
                forma_pagamento = str(val).strip()
            if fid == "origem_da_pe_a" and val:
                origem_peca = str(val).strip()

            # Campo "Valor" (currency) — fonte primária para comparação R3
            if ftype == "currency" and val and label.lower() == "valor":
                valor_card = _parse_currency_br(val)

            # Campo "Descrição das Peças"
            if (
                ftype == "long_text"
                and val
                and "descri" in label.lower()
                and "pe" in label.lower()
            ):
                descricao_pecas = str(val)

            # Anexo "Orçamento Cília" — usado como substituto enquanto API
            # do Cilia não está disponível. Normaliza acentos para casar
            # "Cília", "cilia", "CÍLIA" sem depender de cedilha/case.
            if ftype == "attachment" and val and "cilia" in _norm_label(label):
                anexo_cilia_url = _primeira_url(val) or _primeira_url(f.get("array_value"))

        # Fallback: primeira URL de attachments se o campo não foi identificado
        if not anexo_url:
            for att in node.get("attachments") or []:
                u = att.get("url") or att.get("path")
                if u and (u.endswith(".pdf") or "pdf" in u.lower()):
                    anexo_url = u
                    break

        phase = node.get("current_phase") or {}
        created_at_raw = node.get("created_at")
        created_at: datetime | None = None
        if created_at_raw:
            try:
                created_at = datetime.fromisoformat(
                    str(created_at_raw).replace("Z", "+00:00")
                )
            except ValueError:
                logger.debug("created_at inválido em card %s: %s", node.get("id"), created_at_raw)

        return CardPipefy(
            id=str(node["id"]),
            title=str(node.get("title") or ""),
            phase_id=str(phase.get("id")) if phase.get("id") else None,
            phase_name=phase.get("name"),
            campos=campos,
            codigo_oc=codigo_oc,
            anexo_oc_url=anexo_url,
            anexo_cilia_url=anexo_cilia_url,
            valor_card=valor_card,
            descricao_pecas=descricao_pecas,
            created_at=created_at,
            forma_pagamento=forma_pagamento,
            origem_peca=origem_peca,
        )

    # ---------- pipe principal: cards de cancelamento ----------

    async def listar_cards_cancelamento_pipe_principal(
        self,
    ) -> list[dict[str, Any]]:
        """Varre as duas fases de cancelamento do pipe principal e
        retorna uma lista de dicts no formato esperado por
        `db.atualizar_cache_cancelamentos`:

            {
              "placa_normalizada": "MWE7258",
              "card_id": "1234567890",
              "tipo": "informacoes_incorretas" | "cancelado",
              "fase_atual": "Informações Incorretas" | "Cancelados",
            }

        Reusa `listar_cards_fase()` (que já parseia os cards) para as
        chaves `informacoes_incorretas` e `cancelados` do pipefy_ids.
        A placa vem do `card.title` (já normalizado, sem hífen) com
        fallback defensivo de remover hífen/espaço/upper.
        """
        out: list[dict[str, Any]] = []
        for tipo, chave in (
            ("informacoes_incorretas", "informacoes_incorretas"),
            ("cancelado", "cancelados"),
        ):
            try:
                cards = await self.listar_cards_fase(chave)
            except Exception as e:
                logger.warning(
                    "Falha ao listar fase %s do pipe principal: %s", chave, e
                )
                continue
            for c in cards:
                placa_norm = (
                    (c.title or "").replace("-", "").replace(" ", "").upper()
                )
                if not placa_norm:
                    continue
                out.append(
                    {
                        "placa_normalizada": placa_norm,
                        "card_id": str(c.id),
                        "tipo": tipo,
                        "fase_atual": c.phase_name,
                        "descricao_pecas": c.descricao_pecas,
                        "codigo_oc": c.codigo_oc,
                    }
                )
        logger.info(
            "Cancelamentos no pipe principal: %d cards "
            "(informacoes_incorretas + cancelados)",
            len(out),
        )
        return out

    # ---------- pipe de Devolução de Peças ----------

    LIST_PIPE_CARDS_QUERY = """
    query($pipeId: ID!, $first: Int!, $after: String) {
      cards(pipe_id: $pipeId, first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            done
            current_phase { id name }
            fields {
              field { id label type }
              value
            }
          }
        }
      }
    }
    """

    async def listar_devolucoes_abertas(
        self,
        pipe_id: int | None = None,
        *,
        max_cards: int = 1000,
    ) -> list[dict[str, Any]]:
        """Lista todos os cards `done=false` do pipe de Devolução de Peças.

        Retorna uma lista de dicts no formato esperado por
        `db.atualizar_cache_devolucoes`:

            {
              "placa_normalizada": "MWE7258",   # sem hífen
              "card_id": "1089889305",
              "n_oc": "1597340" | None,
              "peca_descricao": "BARRA DIR..." | None,
              "fase_atual": "Peça em Estoque" | None,
            }

        Não levanta exceção em caso de campos faltantes — ignora cards
        sem placa preenchida (logando warning) para não quebrar a R2.
        """
        pid = pipe_id or settings.pipefy_pipe_devolucao_id
        out: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            data = await self._gql(
                self.LIST_PIPE_CARDS_QUERY,
                {"pipeId": str(pid), "first": 50, "after": after},
            )
            conn_data = data.get("cards") or {}
            for edge in conn_data.get("edges") or []:
                node = edge.get("node") or {}
                if node.get("done"):
                    # `done=true` significa fase terminal (Concluído,
                    # Não Localizada, Cancelado). Não nos interessa.
                    continue
                placa_raw: str | None = None
                n_oc: str | None = None
                peca_desc: str | None = None
                cod_peca: str | None = None
                motivo: str | None = None
                for f in node.get("fields") or []:
                    fdef = f.get("field") or {}
                    fid = fdef.get("id")
                    val = f.get("value")
                    if not val:
                        continue
                    if fid == "placa":
                        placa_raw = str(val).strip()
                    elif fid == "n_oc":
                        n_oc = str(val).strip()
                    elif fid == "cite_as_pe_as_a_serem_devolvidas":
                        peca_desc = str(val).strip()
                    elif fid == "cod":
                        cod_peca = str(val).strip()
                    elif fid == "motivo_devolu_o":
                        motivo = str(val).strip()
                if not placa_raw:
                    logger.debug(
                        "Devolução %s sem placa preenchida — ignorada",
                        node.get("id"),
                    )
                    continue
                # Normaliza placa: remove hífen + uppercase + sem espaços.
                # No pipe Devolução já vem sem hífen (segundo nosso teste),
                # mas normalizamos defensivamente para casar com o Club.
                placa_norm = (
                    placa_raw.replace("-", "").replace(" ", "").upper()
                )
                phase = node.get("current_phase") or {}
                out.append(
                    {
                        "placa_normalizada": placa_norm,
                        "card_id": str(node.get("id")),
                        "n_oc": n_oc,
                        "peca_descricao": peca_desc,
                        "fase_atual": phase.get("name"),
                    }
                )
            page = conn_data.get("pageInfo") or {}
            if not page.get("hasNextPage") or len(out) >= max_cards:
                break
            after = page.get("endCursor")

        logger.info(
            "Pipefy Devolução: %d cards em aberto (pipe %s)", len(out), pid
        )
        return out

    # ---------- download de anexo + extração de valor ----------

    async def baixar_anexo(self, url: str) -> bytes:
        """Baixa um anexo (o Pipefy usa URLs assinadas de S3)."""
        async with httpx.AsyncClient(timeout=60.0) as c:
            r = await c.get(url)
            r.raise_for_status()
            return r.content

    async def extrair_valor_pdf(self, card: CardPipefy) -> Decimal | None:
        if not card.anexo_oc_url:
            logger.warning("Card %s sem anexo de OC", card.id)
            return None
        try:
            conteudo = await self.baixar_anexo(card.anexo_oc_url)
        except Exception as e:
            logger.warning("Falha ao baixar anexo do card %s: %s", card.id, e)
            return None
        # Usa versão async (thread pool) para não bloquear o event loop
        valor = await extrair_valor_total_async(conteudo)
        card.valor_extraido_pdf = valor
        return valor

    # ---------- escrita ----------

    UPDATE_FIELD_MUTATION = """
    mutation($cardId: ID!, $fieldId: ID!, $newValue: [UndefinedInput!]!) {
      updateCardField(input: {
        card_id: $cardId,
        field_id: $fieldId,
        new_value: $newValue
      }) { success }
    }
    """

    async def update_card_field(
        self, card_id: str, field_chave: str, value: Any
    ) -> bool:
        field_id = self._ids().campo_id(field_chave)
        if self.dry_run:
            logger.info(
                "[dry_run] updateCardField card=%s field=%s(%s) value=%s",
                card_id, field_chave, field_id, value,
            )
            return True
        new_value = value if isinstance(value, list) else [value]
        data = await self._gql(
            self.UPDATE_FIELD_MUTATION,
            {"cardId": card_id, "fieldId": field_id, "newValue": new_value},
        )
        return bool((data.get("updateCardField") or {}).get("success"))

    MOVE_CARD_MUTATION = """
    mutation($cardId: ID!, $destId: ID!) {
      moveCardToPhase(input: {
        card_id: $cardId,
        destination_phase_id: $destId
      }) {
        card { id current_phase { id name } }
      }
    }
    """

    async def mover_card(self, card_id: str, fase_chave: str) -> bool:
        dest_id = self._ids().fase_id(fase_chave)
        if self.dry_run:
            logger.info(
                "[dry_run] moveCardToPhase card=%s → %s(%s)",
                card_id, fase_chave, dest_id,
            )
            return True
        data = await self._gql(
            self.MOVE_CARD_MUTATION, {"cardId": card_id, "destId": dest_id}
        )
        return bool(data.get("moveCardToPhase"))


# ---------- utils ----------

def _parse_currency_br(v: Any) -> Decimal | None:
    """Converte valor BR ('2.575,00' ou '107,43') em Decimal."""
    if v is None or v == "":
        return None
    s = str(v).strip().replace("R$", "").replace(" ", "")
    # Se tem vírgula, assume formato BR (ponto=milhar, vírgula=decimal)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _primeira_url(v: Any) -> str | None:
    if not v:
        return None
    if isinstance(v, str):
        # Pipefy às vezes retorna JSON-string: '["https://..."]'
        s = v.strip()
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list) and arr:
                    return str(arr[0])
            except json.JSONDecodeError:
                pass
        if s.startswith("http"):
            return s
    if isinstance(v, list) and v:
        return str(v[0])
    return None
