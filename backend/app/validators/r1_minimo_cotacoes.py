"""R1 — Minimo de 3 cotacoes POR PECA.

Regra de negocio (confirmada pelo dono do processo em 2026-04-15):
cada peca na cotacao deve ter pelo menos 3 fornecedores que ofertaram.
Se qualquer peca tiver menos de 3 ofertas, a OC e sinalizada.

Implementacao:
- Usa `ProdutoCotacao.qtd_cotacoes_peca`, populado pelo orchestrator
  atraves de `/api/v2/requests/{id}/products/offers`.
- Se `qtd_cotacoes_peca` e None em TODAS as pecas (endpoint indisponivel),
  cai no fallback historico: validar pelo total global
  `len(contexto.concorrentes) < minimo` — mesmo comportamento que existia
  antes da implementacao por-peca. Garante que falhas do endpoint novo
  nao deixam a regra permissiva demais.
"""
from __future__ import annotations

from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


class R1MinimoCotacoes(Regra):
    codigo = "R1"
    nome = "Minimo de 3 cotacoes"
    minimo = 3

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        produtos = contexto.produtos_cotacao or []
        # Caminho 1: temos dados por-peca do endpoint v2/requests/offers
        pecas_com_dados = [
            p for p in produtos if p.qtd_cotacoes_peca is not None
        ]
        if pecas_com_dados:
            insuficientes = [
                p for p in pecas_com_dados
                if (p.qtd_cotacoes_peca or 0) < self.minimo
            ]
            if not insuficientes:
                return []
            # Monta descricao listando as pecas problema
            partes = [
                f"{p.descricao or p.produto_id or '(sem nome)'} "
                f"({p.qtd_cotacoes_peca or 0} cotacao(oes))"
                for p in insuficientes
            ]
            resumo = "; ".join(partes)
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo=(
                        f"{len(insuficientes)} peca(s) com menos de "
                        f"{self.minimo} cotacoes"
                    ),
                    descricao=(
                        f"A cotacao {contexto.oc.id_cotacao} tem peca(s) "
                        f"com menos de {self.minimo} fornecedores ofertando: "
                        f"{resumo}. Comprador precisa justificar no card do Pipefy."
                    ),
                    severidade=Severidade.ERRO,
                    dados={
                        "minimo": self.minimo,
                        "pecas_insuficientes": [
                            {
                                "produto_id": p.produto_id,
                                "descricao": p.descricao,
                                "qtd_cotacoes_peca": p.qtd_cotacoes_peca,
                            }
                            for p in insuficientes
                        ],
                    },
                )
            ]

        # Caminho 2 (fallback): endpoint por-peca indisponivel —
        # valida pelo total global de concorrentes (comportamento legado).
        qtd = len(contexto.concorrentes)
        if qtd >= self.minimo:
            return []

        return [
            Divergencia(
                regra=self.codigo,
                titulo=f"Apenas {qtd} cotacao(oes)",
                descricao=(
                    f"A cotacao {contexto.oc.id_cotacao} teve apenas {qtd} "
                    f"fornecedor(es) concorrentes. Minimo exigido: {self.minimo}. "
                    "Comprador precisa justificar no card do Pipefy. "
                    "(Modo fallback global — dados por-peca indisponiveis)"
                ),
                severidade=Severidade.ERRO,
                dados={"qtd_cotacoes": qtd, "minimo": self.minimo, "modo": "global"},
            )
        ]
