"""Serviço de histórico de produtos por OC para a R2 cross-time.

Mantém a tabela `historico_produtos_oc` (SQLite local) populada
incrementalmente: a cada validação, garante que os últimos
`R2_JANELA_DIAS` dias antes do D-1 estão presentes. Faz backfill
automático na 1ª execução; nas subsequentes só baixa os dias faltantes.

A intenção é que a R2 cross-time consulte o histórico via SQL local
(<100ms) em vez de bater no Club a cada peça validada.
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta
from typing import Any

from app.clients.club_client import ClubClient
from app.db import (
    dias_ja_processados,
    dias_presentes_no_historico,
    marcar_dia_processado,
    registrar_historico_produtos,
)
from app.logging_setup import get_logger
from app.models import OrdemCompra
from app.utils.chave_produto import chave_produto_de_dict

logger = get_logger(__name__)


def _chave_produto_dict(p: dict[str, Any]) -> str:
    """Mesma lógica de chave usada pela R2 (EAN > código > descrição).

    Delega para a função canônica em utils.chave_produto.
    Mantida como wrapper para compatibilidade com orchestrator.
    """
    return chave_produto_de_dict(p)


def _item_para_chave_dict(item: dict[str, Any]) -> dict[str, Any]:
    """Converte um `item` do `get_order_details(...).items[*]` no formato
    esperado por `_chave_produto_dict` (ean / cod_interno / descricao)."""
    product = item.get("product") or {}
    return {
        "ean": product.get("ean"),
        "cod_interno": product.get("internal_code"),
        "descricao": product.get("name") or item.get("descricao"),
    }


def _extrair_linhas_de_pedidos(
    pedidos: list[dict[str, Any]],
    dia: date,
) -> list[dict[str, Any]]:
    """Converte lista de pedidos (com items inline) em linhas para
    `registrar_historico_produtos`. Funciona tanto com resposta v3
    (já normalizada por _normalizar_pedido_v3) quanto v1+details."""
    linhas: list[dict[str, Any]] = []
    dia_iso = dia.isoformat()
    for raw in pedidos:
        items = raw.get("items") or raw.get("itens") or []
        id_pedido = str(raw.get("id_pedido") or raw.get("id") or "").strip()
        if not id_pedido:
            continue
        identificador = (
            raw.get("identificador") or raw.get("identifier") or ""
        )
        placa_norm = (
            str(identificador).replace("-", "").replace(" ", "").upper()
        )
        forn = raw.get("fornecedor") or {}
        forn_id = str(forn.get("for_id") or raw.get("for_id") or "") or None
        forn_nome = forn.get("for_nome") or raw.get("fornecedor_nome")
        for item in items:
            chave_input = _item_para_chave_dict(item)
            chave = _chave_produto_dict(chave_input)
            product = item.get("product") or {}
            linhas.append({
                "data_oc": dia_iso,
                "id_pedido": id_pedido,
                "id_cotacao": str(raw.get("id_cotacao") or "") or None,
                "placa_normalizada": placa_norm or "",
                "identificador": identificador or None,
                "chave_produto": chave,
                "descricao": product.get("name") or chave_input.get("descricao"),
                "fornecedor_id": forn_id,
                "fornecedor_nome": forn_nome,
                "quantidade": float(item.get("quantity") or 0),
                "card_pipefy_id": None,
            })
    return linhas


async def _coletar_dia(
    club: ClubClient,
    dia: date,
    semaforo: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Baixa os pedidos de um dia + seus ITENS e retorna linhas prontas
    para `registrar_historico_produtos`.

    Estratégia (v3 primeiro, v1 como fallback):
      1. Tenta `listar_pedidos_v3(dia)` que retorna OCs COM items, seller
         e buyer inline numa única chamada por página — elimina N chamadas
         individuais a `get_order_details`.
      2. Se v3 falhar, faz fallback para v1 (`listar_pedidos` + N×
         `get_order_details`), comportamento idêntico ao anterior.

    Filtramos OCs com `status != "P"` (defensivo: hoje só vimos "P" no
    listarpedidos, mas se aparecer "C"/cancelado, ignoramos).
    """
    # ---- Tentativa v3 (batch com items inline) ----
    try:
        pedidos = await club.listar_pedidos_v3(dia)
        if pedidos:
            pedidos = [p for p in pedidos if (p.get("status") or "P") == "P"]
            linhas = _extrair_linhas_de_pedidos(pedidos, dia)
            logger.debug(
                "Histórico %s: v3 retornou %d pedidos → %d linhas",
                dia, len(pedidos), len(linhas),
            )
            return linhas
    except Exception as e:
        logger.warning(
            "Falha ao usar v3 para listar pedidos de %s, "
            "tentando fallback v1: %s", dia, e,
        )

    # ---- Fallback v1 (listar_pedidos + get_order_details por OC) ----
    try:
        pedidos = await club.listar_pedidos(dia)
    except Exception as e:
        logger.warning("Falha ao listar pedidos de %s (v1): %s", dia, e)
        return []
    if not pedidos:
        return []

    # Filtra canceladas / status não-aprovados
    pedidos = [p for p in pedidos if (p.get("status") or "P") == "P"]

    async def _items_de(raw: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        id_pedido = raw.get("id_pedido") or raw.get("id")
        if not id_pedido:
            return raw, []
        async with semaforo:
            try:
                det = await club.get_order_details(id_pedido)
            except Exception as e:
                logger.debug(
                    "Falha get_order_details OC %s: %s", id_pedido, e
                )
                return raw, []
        return raw, det.get("items") or []

    tarefas = [_items_de(p) for p in pedidos]
    resultados = await asyncio.gather(*tarefas)

    # Injeta items no raw para reutilizar _extrair_linhas_de_pedidos
    pedidos_com_items = []
    for raw, items in resultados:
        raw_copy = dict(raw)
        raw_copy["items"] = items
        pedidos_com_items.append(raw_copy)

    return _extrair_linhas_de_pedidos(pedidos_com_items, dia)


async def garantir_historico(
    club: ClubClient,
    *,
    ate_dia: date,
    dias_janela: int,
    concorrencia: int = 15,
    time_budget_seconds: float = 480.0,
) -> dict:
    """Garante que `historico_produtos_oc` cobre [ate_dia - dias_janela, ate_dia].

    Processa os dias faltantes em CHUNKS de 30 dias (paralelos com
    semáforo), respeitando um `time_budget_seconds` global — se o budget
    estourar entre chunks, retorna status parcial em vez de continuar
    indefinidamente. Isso garante que a validação principal não estoure
    o timeout global por culpa do backfill.

    Retorna um dict com o status do backfill:

    ```
    {
        "completo": bool,              # cobre a janela inteira?
        "dias_cobertos": int,          # dias atualmente no histórico na janela
        "dias_necessarios": int,       # = dias_janela
        "dias_baixados_agora": int,    # quantos dias efetivamente foram baixados nesta execução
        "chunks_processados": int,     # quantos chunks de 30 dias foram processados
        "erro": str | None,            # mensagem de erro (se o loop parou por exceção)
    }
    ```

    Notas:
      - `INSERT OR IGNORE` garante idempotência se o usuário rodar de novo
      - Erro num dia específico apenas loga warning (dentro de `_coletar_dia`);
        erros catastróficos (ex: DB schema, connection) sobem como exceção.
    """
    inicio_ts = time.monotonic()
    inicio_periodo = ate_dia - timedelta(days=dias_janela)
    fim_periodo = ate_dia

    def _contar_cobertos() -> int:
        return len(
            dias_presentes_no_historico(
                inicio_periodo.isoformat(), fim_periodo.isoformat()
            )
        )

    todos_dias = [
        (inicio_periodo + timedelta(days=i)).isoformat()
        for i in range((fim_periodo - inicio_periodo).days + 1)
    ]
    presentes = dias_presentes_no_historico(
        inicio_periodo.isoformat(), fim_periodo.isoformat()
    )
    # Dias ja processados (mesmo que vazios) nao precisam ser re-consultados
    processados = dias_ja_processados(
        inicio_periodo.isoformat(), fim_periodo.isoformat()
    )
    # Faltantes = dias que nao estao presentes E nao foram processados antes
    faltantes = [d for d in todos_dias if d not in presentes and d not in processados]

    dias_necessarios = len(todos_dias)

    if not faltantes:
        logger.info(
            "Histórico de produtos: já cobre %s a %s (%d dias presentes)",
            inicio_periodo, fim_periodo, len(presentes),
        )
        return {
            "completo": True,
            "dias_cobertos": len(presentes),
            "dias_necessarios": dias_necessarios,
            "dias_baixados_agora": 0,
            "chunks_processados": 0,
            "erro": None,
        }

    logger.info(
        "Histórico de produtos: %d dias faltantes na janela %s a %s — "
        "baixando do Club em chunks de 30 dias (budget=%ds)...",
        len(faltantes), inicio_periodo, fim_periodo, int(time_budget_seconds),
    )

    semaforo = asyncio.Semaphore(concorrencia)
    chunk_size = 30
    chunks = [
        faltantes[i : i + chunk_size]
        for i in range(0, len(faltantes), chunk_size)
    ]

    dias_baixados_agora = 0
    chunks_processados = 0
    erro: str | None = None

    for idx, chunk in enumerate(chunks, start=1):
        decorrido = time.monotonic() - inicio_ts
        if decorrido > time_budget_seconds:
            logger.warning(
                "Histórico de produtos: time budget estourado "
                "(%.0fs > %.0fs) após %d chunks — retornando status parcial.",
                decorrido, time_budget_seconds, chunks_processados,
            )
            break

        logger.info(
            "Histórico: processando chunk %d/%d (%d dias, decorrido=%.0fs)",
            idx, len(chunks), len(chunk), decorrido,
        )

        async def _baixar_dia(dia_iso: str) -> int:
            dia = date.fromisoformat(dia_iso)
            linhas = await _coletar_dia(club, dia, semaforo)
            if linhas:
                inseridas = registrar_historico_produtos(linhas)
                # Marca como processado COM dados
                marcar_dia_processado(dia_iso, tinha_dados=True)
                return inseridas
            # Marca como processado SEM dados (nao re-consulta)
            marcar_dia_processado(dia_iso, tinha_dados=False)
            return 0

        try:
            resultados = await asyncio.gather(
                *[_baixar_dia(d) for d in chunk],
                return_exceptions=True,
            )
        except Exception as e:
            # Catastrófico: erro fora do gather (ex: semaforo morto).
            # Propaga para o orchestrator decidir se quebra a validação.
            erro = f"Falha catastrófica no chunk {idx}: {e}"
            logger.exception(erro)
            break

        for r in resultados:
            if isinstance(r, Exception):
                logger.warning("Histórico: dia falhou no chunk %d: %s", idx, r)
                continue
            if r > 0:
                dias_baixados_agora += 1

        chunks_processados += 1

    dias_cobertos_final = _contar_cobertos()
    completo = dias_cobertos_final >= dias_necessarios

    logger.info(
        "Histórico de produtos: fim — cobertos=%d/%d, chunks=%d/%d, "
        "dias_baixados=%d, completo=%s",
        dias_cobertos_final, dias_necessarios,
        chunks_processados, len(chunks),
        dias_baixados_agora, completo,
    )

    return {
        "completo": completo,
        "dias_cobertos": dias_cobertos_final,
        "dias_necessarios": dias_necessarios,
        "dias_baixados_agora": dias_baixados_agora,
        "chunks_processados": chunks_processados,
        "erro": erro,
    }
