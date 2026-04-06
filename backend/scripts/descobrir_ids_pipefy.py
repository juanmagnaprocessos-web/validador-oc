"""Script introspectivo — descobre IDs de fases e campos do Pipefy.

Roda **uma única vez** (ou quando a configuração do pipe mudar).
Gera `config/pipefy_ids.json` com o mapeamento:

{
  "pipe_id": 305587531,
  "pipe_name": "SINISTRO - LOGÍSTICA",
  "phases": {
    "Validação Ordem de Compra": { "id": "...", "fields": { "Código da OC": {...} } },
    ...
  },
  "fase_destino": {
    "Aguardar Peças": "...",
    "Programar Pagamento": "...",
    "Compras Mercado Livre": "...",
    "Informações Incorretas": "..."
  },
  "campos_validacao": {
    "codigo_oc": "...",
    "peca_duplicada": "...",
    "abatimento_fornecedor": "...",
    "validacao_concluida_por": "...",
    "validacao_concluida": "...",
    "ordem_de_compra_pdf": "..."
  }
}

Uso:
    python -m scripts.descobrir_ids_pipefy
"""
from __future__ import annotations

import json
import sys
import unicodedata

import httpx
from rich.console import Console
from rich.table import Table

from app.config import settings

# force_terminal + legacy_windows=False garante UTF-8 no Windows.
console = Console(legacy_windows=False, force_terminal=True)


QUERY = """
query($pipeId: ID!) {
  pipe(id: $pipeId) {
    id
    name
    phases {
      id
      name
      fields {
        id
        label
        type
        internal_id
      }
    }
    start_form_fields {
      id
      label
      type
      internal_id
    }
  }
}
"""


def _norm(s: str) -> str:
    """Normaliza string para comparação tolerante (sem acentos, lowercase)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def _buscar_fase(phases: list[dict], nome_alvo: str) -> dict | None:
    alvo = _norm(nome_alvo)
    for p in phases:
        if _norm(p["name"]) == alvo:
            return p
    # fallback: contains
    for p in phases:
        if alvo in _norm(p["name"]):
            return p
    return None


def _buscar_campo(fields: list[dict], *nomes: str) -> dict | None:
    for nome in nomes:
        alvo = _norm(nome)
        for f in fields:
            if _norm(f["label"]) == alvo:
                return f
        for f in fields:
            if alvo in _norm(f["label"]):
                return f
    return None


def main() -> int:
    if not settings.pipefy_token or "SUBSTITUIR" in settings.pipefy_token:
        console.print(
            "[red]PIPEFY_TOKEN não configurado. Edite .env primeiro.[/red]"
        )
        return 1

    console.print(
        f"[cyan]Consultando pipe {settings.pipe_id} em {settings.pipefy_api_url}...[/cyan]"
    )

    resp = httpx.post(
        settings.pipefy_api_url,
        json={"query": QUERY, "variables": {"pipeId": str(settings.pipe_id)}},
        headers={
            "Authorization": f"Bearer {settings.pipefy_token}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    )

    if resp.status_code != 200:
        console.print(f"[red]HTTP {resp.status_code}: {resp.text[:500]}[/red]")
        return 2

    payload = resp.json()
    if "errors" in payload:
        console.print("[red]GraphQL errors:[/red]")
        console.print(payload["errors"])
        return 3

    pipe = payload["data"]["pipe"]
    phases = pipe["phases"]

    console.print(
        f"[green]OK[/green] pipe [bold]{pipe['name']}[/bold] ({pipe['id']}) "
        f"— {len(phases)} fases"
    )

    # Tabela de fases
    tbl = Table(title="Fases do pipe", show_lines=False)
    tbl.add_column("ID", style="cyan")
    tbl.add_column("Nome", style="white")
    tbl.add_column("# campos", justify="right")
    for p in phases:
        tbl.add_row(p["id"], p["name"], str(len(p["fields"])))
    console.print(tbl)

    # Identificar fases de interesse
    # Alguns nomes reais no Pipefy diferem do briefing (ex: "Compra" no
    # singular). O matcher tolera diferenças mas aliases ajudam quando o
    # nome muda completamente.
    fases_alvo = {
        "validacao": "Validação Ordem de Compra",
        "aguardar_pecas": "Aguardar Peças",
        "programar_pagamento": "Programar Pagamento",
        "compras_ml": "Compra Mercado Livre",  # nome real no Pipefy (singular)
        "informacoes_incorretas": "Informações Incorretas",
    }

    mapping_fases: dict[str, str | None] = {}
    for chave, nome in fases_alvo.items():
        f = _buscar_fase(phases, nome)
        mapping_fases[chave] = f["id"] if f else None
        marker = "[green]OK[/green]" if f else "[red]FALTA[/red]"
        console.print(
            f"  {marker} {chave}: {nome} -> "
            f"{f['id'] if f else 'NAO ENCONTRADA'}"
        )

    # Campos relevantes (buscar em TODAS as fases, pois Pipefy pode ter fields
    # globais no pipe ou específicos por fase)
    all_fields: list[dict] = list(pipe.get("start_form_fields") or [])
    for p in phases:
        all_fields.extend(p["fields"])

    campos_alvo = {
        "codigo_oc": ("Código da OC", "Codigo da OC", "Código OC"),
        "peca_duplicada": ("Peça duplicada?", "Peça Duplicada", "Peca Duplicada"),
        "abatimento_fornecedor": (
            "Abatimento fornecedor?",
            "Abatimento Fornecedor",
        ),
        "validacao_concluida_por": (
            "Validação da Oc concluída por",
            "Validação da OC concluída por",
            "Validação concluída por",
            "Validacao concluida por",
        ),
        "validacao_concluida": (
            "Validação concluída?",
            "Validação concluída",
            "Validacao concluida",
        ),
        "ordem_de_compra_pdf": ("Ordem de compra", "Ordem de Compra"),
        "forma_de_entrega": ("Forma de Entrega", "Forma de entrega"),
        "fornecedor": ("Fornecedor",),
        # Campos descobertos durante a introspecção — úteis no fluxo:
        "justificativa_divergencia": (
            "Informe a negativa da validação",
            "Informe a negativa",
        ),
        "descricao_pecas": ("Descrição das Peças", "Descricao das Pecas"),
    }

    mapping_campos: dict[str, dict | None] = {}
    for chave, nomes in campos_alvo.items():
        f = _buscar_campo(all_fields, *nomes)
        mapping_campos[chave] = (
            {"id": f["id"], "label": f["label"], "type": f.get("type")} if f else None
        )
        marker = "[green]OK[/green]" if f else "[yellow]?[/yellow]"
        console.print(
            f"  {marker} campo {chave}: {nomes[0]} -> "
            f"{f['id'] if f else 'NAO ENCONTRADO'}"
        )

    # Tabela detalhada da fase "Validação Ordem de Compra"
    fase_validacao = _buscar_fase(phases, "Validação Ordem de Compra")
    if fase_validacao:
        tbl2 = Table(
            title=f'Campos da fase "{fase_validacao["name"]}"', show_lines=False
        )
        tbl2.add_column("ID", style="cyan")
        tbl2.add_column("Label")
        tbl2.add_column("Tipo")
        for f in fase_validacao["fields"]:
            tbl2.add_row(f["id"], f["label"], f.get("type") or "")
        console.print(tbl2)

    # Salvar
    out = {
        "pipe_id": int(pipe["id"]),
        "pipe_name": pipe["name"],
        "phases": {
            p["name"]: {
                "id": p["id"],
                "fields": {
                    f["label"]: {
                        "id": f["id"],
                        "type": f.get("type"),
                        "internal_id": f.get("internal_id"),
                    }
                    for f in p["fields"]
                },
            }
            for p in phases
        },
        "fase_destino": mapping_fases,
        "campos_validacao": mapping_campos,
    }

    out_path = settings.pipefy_ids_full_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    console.print(f"\n[green]Salvo em:[/green] {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
