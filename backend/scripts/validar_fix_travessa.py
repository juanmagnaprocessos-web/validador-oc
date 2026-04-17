"""Valida o fix da R2 cross-time para a OC 2048810 (TRAVESSA INF, OPB-9H43).

Testa o caminho NOVO (relatorio Club por placa) em isolamento — sem rodar o
orchestrator completo:

  1) Chama listar_produtos_por_placa para OPB-9H43 na janela R2 (210 dias).
  2) Aplica _normalizar_relatorio_produtos_placa.
  3) Indexa por chave_produto.
  4) Chama detectar_reincidencias com produtos da OC atual (TRAVESSA, etc).
  5) Mostra divergencias detectadas.

Uso:
    python -m scripts.validar_fix_travessa
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.clients.club_client import ClubClient
from app.config import settings
from app.models import ProdutoCotacao
from app.services.orchestrator import (
    _formatar_placa_para_club,
    _normalizar_relatorio_produtos_placa,
    _indexar_historico_por_chave,
)
from app.validators.r2_duplicidade import detectar_reincidencias


PLACA = "OPB9H43"
ID_PEDIDO_ATUAL = "2048810"
DATA_D1 = date(2026, 4, 16)


async def main() -> None:
    club = ClubClient()
    await club.authenticate()

    janela_ini = DATA_D1 - timedelta(days=settings.r2_janela_dias)
    janela_fim = DATA_D1
    placa_fmt = _formatar_placa_para_club(PLACA)
    print(f"Buscando relatorio Club para {placa_fmt} entre {janela_ini} e {janela_fim}")

    raw = await club.listar_produtos_por_placa(placa_fmt, janela_ini, janela_fim)
    print(f"Itens crus do endpoint: {len(raw)}")

    items = _normalizar_relatorio_produtos_placa(
        PLACA, raw, id_pedido_atual=ID_PEDIDO_ATUAL,
    )
    print(f"Itens normalizados (excluindo OC {ID_PEDIDO_ATUAL}): {len(items)}")

    # Mostrar items com chave ean:132888 (a TRAVESSA esperada)
    travessa_items = [it for it in items if it["chave_produto"] == "ean:132888"]
    print(f"\nItems da TRAVESSA INF (ean:132888) no historico: {len(travessa_items)}")
    for it in travessa_items:
        print(f"  -> OC {it['id_pedido']} | {it['data_oc']} | {it['fornecedor_nome']}")

    historico_indexado = _indexar_historico_por_chave(items)
    print(f"\nChaves no historico indexado: {len(historico_indexado)}")

    # Produtos da OC atual (2048810) — TRAVESSA INF + outros
    produtos_oc_atual = [
        ProdutoCotacao(
            descricao="TRAVESSA INF DO PARACHOQUE DIANT",
            ean="132888",
            cod_interno=None,
            quantidade=1,
            valor_unitario=None,
            valor_total=None,
        ),
        ProdutoCotacao(
            descricao="MANGUEIRA INF DO RADIADOR",
            ean="101039",
            cod_interno=None,
            quantidade=1,
            valor_unitario=None,
            valor_total=None,
        ),
        ProdutoCotacao(
            descricao="CONECTOR DE LAMPADA",
            ean="102244",
            cod_interno=None,
            quantidade=1,
            valor_unitario=None,
            valor_total=None,
        ),
    ]

    divs = detectar_reincidencias(
        placa_normalizada=PLACA,
        identificador="OPB-9H43",
        id_pedido_atual=ID_PEDIDO_ATUAL,
        fornecedor_id="7750",  # PEIXINHO
        produtos=produtos_oc_atual,
        data_d1=DATA_D1,
        _historico_pipefy_items=historico_indexado,
    )

    print(f"\n=== R2 cross-time: {len(divs)} divergencia(s) detectada(s) ===")
    for d in divs:
        print(f"  [{d.severidade.name}] {d.titulo}")
        print(f"    {d.descricao}")
        dados = d.dados or {}
        print(
            f"    chave={dados.get('chave_produto')} "
            f"oc_anterior={dados.get('oc_anterior')} "
            f"data={dados.get('data_anterior')} "
            f"forn={dados.get('fornecedor_anterior_nome')}"
        )

    if not any(
        (d.dados or {}).get("chave_produto") == "ean:132888" for d in divs
    ):
        print("\n[FAIL] R2 NAO detectou a TRAVESSA INF como duplicidade")
        sys.exit(1)
    print("\n[OK] R2 detectou a TRAVESSA INF como duplicidade — fix funciona")
    await club._client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
