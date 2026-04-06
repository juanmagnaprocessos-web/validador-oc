"""R5 — Fornecedor ativo.

Verifica `fornecedor.for_status` == "1" (ativo) E `fornecedor.for_excluido` == "0".
"""
from __future__ import annotations

from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


class R5Fornecedor(Regra):
    codigo = "R5"
    nome = "Fornecedor ativo"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        forn = contexto.oc.fornecedor

        if not forn:
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo="Fornecedor ausente na OC",
                    descricao="A OC não possui dados do fornecedor.",
                    severidade=Severidade.ERRO,
                )
            ]

        out: list[Divergencia] = []

        if str(forn.for_status or "").strip() != "1":
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Fornecedor inativo: {forn.for_nome}",
                    descricao=(
                        f"Fornecedor '{forn.for_nome}' (ID {forn.for_id}) "
                        f"tem for_status='{forn.for_status}' (esperado '1' = ativo)."
                    ),
                    severidade=Severidade.ERRO,
                    dados={
                        "for_id": forn.for_id,
                        "for_nome": forn.for_nome,
                        "for_status": forn.for_status,
                    },
                )
            )

        if str(forn.for_excluido or "0").strip() != "0":
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Fornecedor excluído: {forn.for_nome}",
                    descricao=(
                        f"Fornecedor '{forn.for_nome}' tem "
                        f"for_excluido='{forn.for_excluido}' (esperado '0')."
                    ),
                    severidade=Severidade.ERRO,
                    dados={"for_id": forn.for_id, "for_excluido": forn.for_excluido},
                )
            )

        return out
