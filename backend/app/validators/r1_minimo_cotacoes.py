"""R1 — Mínimo de 3 cotações.

Verifica em `concorrentes[]` retornado por /api/getconcorrentescotacao.
Se houver menos de 3 fornecedores, a OC é sinalizada e o comprador precisa
justificar no card do Pipefy.
"""
from __future__ import annotations

from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


class R1MinimoCotacoes(Regra):
    codigo = "R1"
    nome = "Mínimo de 3 cotações"
    minimo = 3

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        qtd = len(contexto.concorrentes)
        if qtd >= self.minimo:
            return []

        return [
            Divergencia(
                regra=self.codigo,
                titulo=f"Apenas {qtd} cotação(ões)",
                descricao=(
                    f"A cotação {contexto.oc.id_cotacao} teve apenas {qtd} "
                    f"fornecedor(es) concorrentes. Mínimo exigido: {self.minimo}. "
                    "Comprador precisa justificar no card do Pipefy."
                ),
                severidade=Severidade.ERRO,
                dados={"qtd_cotacoes": qtd, "minimo": self.minimo},
            )
        ]
