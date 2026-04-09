"""Carregamento em bulk do histórico de produtos para R2 cross-time.

Evita o problema N+1: em vez de uma query por produto por OC,
carrega todo o histórico da janela de uma vez e faz lookup O(1) por chave.

Não modifica db.py (outro agente cuida). Importa apenas get_conn.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.db import get_conn


def carregar_historico_bulk(
    placa_normalizada: str,
    data_max: date,
    dias: int,
    ignorar_id_pedido: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Carrega todo histórico de produtos de uma placa nos últimos N dias.

    Retorna dict: chave_produto -> lista de registros (ordenados por
    data_oc DESC).

    Uma única query SQL substitui as N queries que `buscar_reincidencias`
    fazia (uma por produto).
    """
    data_min = data_max - timedelta(days=dias)

    sql = """
        SELECT * FROM historico_produtos_oc
        WHERE placa_normalizada = ?
          AND data_oc >= ?
          AND data_oc <= ?
    """
    params: list[Any] = [
        placa_normalizada,
        data_min.isoformat(),
        data_max.isoformat(),
    ]

    if ignorar_id_pedido:
        sql += " AND id_pedido != ?"
        params.append(ignorar_id_pedido)

    sql += " ORDER BY data_oc DESC, id_pedido DESC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    resultado: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        row_dict = dict(row)
        chave = row_dict["chave_produto"]
        resultado.setdefault(chave, []).append(row_dict)
    return resultado
