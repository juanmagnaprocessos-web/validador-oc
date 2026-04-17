"""Teste exploratorio do endpoint /api/getprodutosrelatoriocliente do Club.

Objetivo: capturar a estrutura JSON real da resposta para podermos escrever
o normalizador (_normalizar_relatorio_produtos_placa) com os campos corretos.

Uso:
    python -m scripts.teste_endpoint_placa OPB-9H43 2025-12-01 2026-04-16
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.clients.club_client import ClubClient


async def main(placa: str, dt_ini: str, dt_fim: str) -> None:
    club = ClubClient()
    await club.authenticate()

    url = f"{club._base_v1}/getprodutosrelatoriocliente"
    params = {
        "groupBy": "pe.id_pedido,pe.id_vendedor",
        "imprimir": "true",
        "ordenar": "data_geracao",
        "tipoorder": "desc",
        "dateIni": dt_ini,
        "dateFim": dt_fim,
        "identifier": placa,
    }
    print(f"GET {url}")
    print(f"params: {params}\n")

    data = await club._request("GET", url, params=params)

    print(f"Tipo raiz: {type(data).__name__}")
    if isinstance(data, dict):
        print(f"Chaves raiz: {list(data.keys())}")
    if isinstance(data, list):
        print(f"Tamanho lista: {len(data)}")

    print("\n=== JSON cru (primeiros 6000 chars) ===")
    print(json.dumps(data, default=str, ensure_ascii=False, indent=2)[:6000])

    # Tentativa de parse: procurar OCs alvo (1396431, 1446402)
    print("\n=== Procurando OCs 1396431 e 1446402 no payload ===")
    raw = json.dumps(data, default=str, ensure_ascii=False)
    for oc_id in ("1396431", "1446402"):
        if oc_id in raw:
            print(f"  [OK] OC {oc_id} encontrada no payload")
        else:
            print(f"  [MISS] OC {oc_id} NAO encontrada")

    await club._client.aclose()


if __name__ == "__main__":
    placa = sys.argv[1] if len(sys.argv) > 1 else "OPB-9H43"
    dt_ini = sys.argv[2] if len(sys.argv) > 2 else "2025-12-01"
    dt_fim = sys.argv[3] if len(sys.argv) > 3 else "2026-04-16"
    asyncio.run(main(placa, dt_ini, dt_fim))
