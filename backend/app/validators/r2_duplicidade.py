"""R2 — Quantidade sem duplicidade.

- Soma quantidades dos produtos da cotação (Club)
- Compara com quantidade de itens do Cilia
- Detecta mesma peça (descrição/EAN) repetida
- Duplicidade legítima: mesma peça para fornecedores diferentes ou devolução
  aberta para a placa — esses casos viram ALERTA, não ERRO
"""
from __future__ import annotations

from collections import defaultdict

from app.config import settings
from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


def _chave_produto(p) -> str:
    """Normaliza chave do produto para detectar duplicidade."""
    ean = getattr(p, "ean", None)
    if ean and str(ean).strip():
        return f"ean:{str(ean).strip()}"
    cod = getattr(p, "cod_interno", None)
    if cod and str(cod).strip():
        return f"cod:{str(cod).strip()}"
    desc = getattr(p, "descricao", "") or ""
    return f"desc:{desc.strip().lower()}"


class R2Duplicidade(Regra):
    codigo = "R2"
    nome = "Quantidade sem duplicidade"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        out: list[Divergencia] = []
        produtos = contexto.produtos_cotacao

        # 1) Detectar duplicidade por chave
        grupos: dict[str, list] = defaultdict(list)
        for p in produtos:
            grupos[_chave_produto(p)].append(p)

        duplicados = {k: v for k, v in grupos.items() if len(v) > 1}
        if duplicados:
            detalhes = []
            for k, itens in duplicados.items():
                qtd_total = sum(float(getattr(i, "quantidade", 0) or 0) for i in itens)
                detalhes.append(
                    f"{k} ({len(itens)}× = qtd total {qtd_total})"
                )
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Peça duplicada: {len(duplicados)} item(ns)",
                    descricao=(
                        "Peças aparecem mais de uma vez na cotação: "
                        + "; ".join(detalhes)
                        + ". Verificar se é para fornecedores diferentes "
                          "(pode ser legítimo) e se há devolução aberta no Pipefy."
                    ),
                    severidade=Severidade.ALERTA,
                    dados={"duplicados": list(duplicados.keys())},
                )
            )

        # 2) Comparar quantidade total Club vs Cilia
        if contexto.orcamento_cilia and contexto.orcamento_cilia.encontrado:
            qtd_club = sum(
                float(getattr(p, "quantidade", 0) or 0) for p in produtos
            )
            qtd_cilia = sum(
                float(i.quantidade or 0) for i in contexto.orcamento_cilia.itens
            )
            if qtd_club and qtd_cilia and abs(qtd_club - qtd_cilia) > 0.01:
                # Cilia em stub → INFO (não bloqueia); em http → ERRO
                sev = (
                    Severidade.ERRO
                    if settings.cilia_mode == "http"
                    else Severidade.INFO
                )
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=(
                            f"Qtd Club ({qtd_club:g}) ≠ Cilia ({qtd_cilia:g})"
                        ),
                        descricao=(
                            f"A soma de quantidades dos produtos da cotação "
                            f"({qtd_club:g}) não bate com o orçamento do Cilia "
                            f"({qtd_cilia:g}) para a placa "
                            f"{contexto.oc.placa_normalizada}."
                            + (
                                ""
                                if settings.cilia_mode == "http"
                                else " [Cilia em modo STUB — informativo]"
                            )
                        ),
                        severidade=sev,
                        dados={"qtd_club": qtd_club, "qtd_cilia": qtd_cilia},
                    )
                )

        return out
