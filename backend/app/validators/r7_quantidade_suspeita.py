"""R7 — Quantidade suspeita (peça com múltiplas unidades na mesma linha).

Peças em par naturais (farol direito/esquerdo, retrovisor, defletor) vêm
do Club como linhas SEPARADAS, com EAN próprio por lado — logo não caem
aqui. Quando uma única linha tem `quantidade > 1` (ex.: 3 litros de óleo,
2 filtros), o analista precisa confirmar manualmente no CILIA que a
quantidade bate com o pedido/placa antes de aprovar. Esta regra emite um
ALERTA por linha suspeita para garantir que a conferência apareça no
dashboard — ela não bloqueia a aprovação automática.
"""
from __future__ import annotations

from app.models import ContextoValidacao, Divergencia, Severidade
from app.utils.chave_produto import chave_produto_de_obj
from app.validators.base import Regra


class R7QuantidadeSuspeita(Regra):
    codigo = "R7"
    nome = "Quantidade suspeita (peça com múltiplas unidades)"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        out: list[Divergencia] = []
        for p in contexto.produtos_cotacao:
            try:
                qtd = float(getattr(p, "quantidade", 0) or 0)
            except (TypeError, ValueError):
                continue
            if qtd <= 1:
                continue

            descricao_peca = getattr(p, "descricao", None) or "(sem descrição)"
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Quantidade > 1: {descricao_peca} (QTD {qtd:g})",
                    descricao=(
                        f"Peça '{descricao_peca}' tem {qtd:g} unidades na mesma "
                        "linha da cotação. Validar no CILIA se a quantidade "
                        "bate com o pedido/placa antes de aprovar."
                    ),
                    severidade=Severidade.ALERTA,
                    dados={
                        "descricao_peca": descricao_peca,
                        "quantidade": qtd,
                        "chave_produto": chave_produto_de_obj(p),
                    },
                )
            )
        return out
