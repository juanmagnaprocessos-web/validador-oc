"""Interface base das regras de validação.

Cada regra implementa `validar(contexto) -> list[Divergencia]`. Regras são
isoladas, puras (sem I/O) e testáveis individualmente.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from app.models import ContextoValidacao, Divergencia


class Regra(ABC):
    """Interface de regra. Implementações não devem fazer I/O — a coleta
    já foi feita pelo orchestrator e chega prontinha em `contexto`."""

    codigo: str = ""  # ex: "R1"
    nome: str = ""

    @abstractmethod
    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        """Retorna lista de divergências (vazia = regra OK)."""


def aplicar_regras(
    regras: list[Regra], contexto: ContextoValidacao
) -> list[Divergencia]:
    """Aplica uma lista de regras em sequência e agrega as divergências."""
    out: list[Divergencia] = []
    for regra in regras:
        try:
            out.extend(regra.validar(contexto))
        except Exception as e:
            # Uma falha na regra não pode derrubar a validação inteira,
            # mas vira divergência crítica que o analista precisa ver.
            out.append(
                Divergencia(
                    regra=regra.codigo or regra.__class__.__name__,
                    titulo=f"Falha interna em {regra.codigo}",
                    descricao=f"Erro ao aplicar regra: {e}",
                    dados={"erro": str(e)},
                )
            )
    return out
