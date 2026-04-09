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
from datetime import date, timedelta
from typing import Any

from app.clients.club_client import ClubClient
from app.db import (
    dias_presentes_no_historico,
    registrar_historico_produtos,
)
from app.logging_setup import get_logger
from app.models import OrdemCompra

logger = get_logger(__name__)


def _chave_produto_dict(p: dict[str, Any]) -> str:
    """Mesma lógica de chave usada pela R2 (EAN > código > descrição)."""
    ean = (p.get("ean") or "").strip() if isinstance(p.get("ean"), str) else ""
    cod = (p.get("cod_interno") or "").strip() if isinstance(p.get("cod_interno"), str) else ""
    desc = (p.get("descricao") or p.get("name") or "")
    if not isinstance(desc, str):
        desc = str(desc)
    if ean:
        return f"ean:{ean}"
    if cod:
        return f"cod:{cod}"
    return f"desc:{desc.strip().lower()}"


def _item_para_chave_dict(item: dict[str, Any]) -> dict[str, Any]:
    """Converte um `item` do `get_order_details(...).items[*]` no formato
    esperado por `_chave_produto_dict` (ean / cod_interno / descricao)."""
    product = item.get("product") or {}
    return {
        "ean": product.get("ean"),
        "cod_interno": product.get("internal_code"),
        "descricao": product.get("name") or item.get("descricao"),
    }


async def _coletar_dia(
    club: ClubClient,
    dia: date,
    semaforo: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Baixa os pedidos de um dia + os ITENS efetivamente comprados em
    cada OC (via `get_order_details`) e retorna linhas prontas para
    `registrar_historico_produtos`.

    IMPORTANTE: usamos `get_order_details(id_pedido).items` em vez de
    `get_produtos_cotacao(id_cotacao)`. A razão: várias OCs podem
    compartilhar a MESMA cotação (split de pedido), e `getprodutoscotacao`
    retorna a cotação INTEIRA — populando o histórico com cada produto
    cotado N vezes (uma para cada OC), gerando falsos positivos enormes
    de "reincidência". O `items` do `get_order_details` traz APENAS o
    que foi efetivamente pedido naquela OC específica, alinhado com o
    relatório `Club > Relatórios > Produtos` filtrado por placa.

    Filtramos OCs com `status != "P"` (defensivo: hoje só vimos "P" no
    listarpedidos, mas se aparecer "C"/cancelado, ignoramos).
    """
    try:
        pedidos = await club.listar_pedidos(dia)
    except Exception as e:
        logger.warning("Falha ao listar pedidos de %s: %s", dia, e)
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

    linhas: list[dict[str, Any]] = []
    dia_iso = dia.isoformat()
    for raw, items in resultados:
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


async def garantir_historico(
    club: ClubClient,
    *,
    ate_dia: date,
    dias_janela: int,
    concorrencia: int = 5,
) -> int:
    """Garante que `historico_produtos_oc` cobre [ate_dia - dias_janela, ate_dia].

    Verifica quais dias da janela já estão presentes no SQLite local e
    baixa apenas os dias faltantes do Club. Retorna o número de linhas
    inseridas (zero se nada faltava).

    Estratégia:
      1. Calcula a lista de dias da janela
      2. Consulta `dias_presentes_no_historico(...)` no SQLite
      3. Para cada dia faltante, chama _coletar_dia em sequência (não
         paralelo, para não estourar o rate limit do Club)
      4. Cada dia internamente paraleliza as chamadas de produtos com
         semáforo, então o ganho de paralelizar dias é pequeno e o risco
         de bater rate limit é alto.
      5. INSERT OR IGNORE garante idempotência se o usuário rodar de novo

    Em caso de erro num dia específico, loga warning e continua — uma
    falha parcial não trava a validação.
    """
    inicio = ate_dia - timedelta(days=dias_janela)
    fim = ate_dia
    todos_dias = [
        (inicio + timedelta(days=i)).isoformat()
        for i in range((fim - inicio).days + 1)
    ]
    presentes = dias_presentes_no_historico(inicio.isoformat(), fim.isoformat())
    faltantes = [d for d in todos_dias if d not in presentes]

    if not faltantes:
        logger.info(
            "Histórico de produtos: já cobre %s a %s (%d dias presentes)",
            inicio, fim, len(presentes),
        )
        return 0

    logger.info(
        "Histórico de produtos: %d dias faltantes na janela %s a %s — "
        "baixando do Club (pode demorar na 1ª execução)...",
        len(faltantes), inicio, fim,
    )

    semaforo = asyncio.Semaphore(concorrencia)
    total_inseridas = 0
    for dia_iso in faltantes:
        dia = date.fromisoformat(dia_iso)
        linhas = await _coletar_dia(club, dia, semaforo)
        if linhas:
            inseridas = registrar_historico_produtos(linhas)
            total_inseridas += inseridas
            logger.debug(
                "Histórico %s: %d linhas (de %d coletadas) inseridas",
                dia_iso, inseridas, len(linhas),
            )

    logger.info(
        "Histórico de produtos: backfill concluído — %d linhas novas",
        total_inseridas,
    )
    return total_inseridas
