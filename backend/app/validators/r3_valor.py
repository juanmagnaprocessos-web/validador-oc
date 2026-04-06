"""R3 — Valor consistente + validação do anexo de OC no Pipefy.

Esta regra tem DOIS objetivos distintos:

  1. **Garantir que a Ordem de Compra foi anexada no Pipefy** — o anexo
     "Ordem de compra" no card é a prova documental. Se não houver anexo,
     é ERRO (a OC não pode seguir para o financeiro sem a documentação).

  2. **Garantir que os valores são consistentes** — compara até 4 fontes:
       a. valor do Club da Cotação (API) — verdade primária
       b. valor do PDF anexado no Pipefy (parsing)
       c. valor do campo "Valor" do card Pipefy (currency estruturado)
       d. valor do orçamento Cilia (quando disponível)

A tolerância é configurável via `R3_TOLERANCIA_CENTAVOS` no .env.
Divergências com Cilia são classificadas como INFO enquanto a API real
não está disponível (hoje o cliente é um stub).

Magnitude da divergência:
  - ≤ R$ 1,00    → "por centavos"
  - ≤ R$ 100,00  → "por frete/ICMS"
  - acima        → "estrutural"
"""
from __future__ import annotations

from decimal import Decimal

from app.config import settings
from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


def _classificar(delta: Decimal) -> str:
    d = abs(delta)
    if d <= Decimal("1.00"):
        return "por centavos"
    if d <= Decimal("100.00"):
        return "por frete/ICMS"
    return "estrutural"


class R3Valor(Regra):
    codigo = "R3"
    nome = "Valor consistente + anexo da OC"

    @property
    def tolerancia(self) -> Decimal:
        cents = settings.r3_tolerancia_centavos
        return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        oc = contexto.oc
        valor_club = oc.valor_pedido
        card = contexto.card_pipefy
        tol = self.tolerancia

        out: list[Divergencia] = []

        # ---- Pré-requisito: valor do Club deve existir ----
        if valor_club is None:
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo="Valor do Club ausente",
                    descricao="Não foi possível obter valor da OC no Club da Cotação.",
                    severidade=Severidade.ERRO,
                )
            ]

        # ---- Check 1: card existe no Pipefy? ----
        if card is None:
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo="Sem card no Pipefy para esta placa",
                    descricao=(
                        f"Nenhum card encontrado na fase 'Validação Ordem de "
                        f"Compra' para a placa {oc.placa_normalizada}. O card "
                        "precisa ser criado antes da validação poder concluir."
                    ),
                    severidade=Severidade.ERRO,
                    dados={"placa": oc.placa_normalizada},
                )
            )
            # Sem card, não conseguimos validar os outros checks — encerra
            return out

        # ---- Check 2: anexo "Ordem de compra" existe? ----
        if not card.anexo_oc_url:
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo="Anexo 'Ordem de compra' ausente no card",
                    descricao=(
                        "O card existe no Pipefy, mas o campo 'Ordem de compra' "
                        "(PDF da OC) não está anexado. O comprador precisa "
                        "gerar e anexar a OC antes da validação."
                    ),
                    severidade=Severidade.ERRO,
                    dados={"card_id": card.id},
                )
            )
            # Segue para os demais checks mesmo sem anexo (campo Valor do card
            # ainda pode ser comparado)

        # ---- Check 3: valor no PDF anexado bate? ----
        if card.anexo_oc_url and card.valor_extraido_pdf is None:
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo="PDF do Pipefy sem valor extraível",
                    descricao=(
                        "O anexo 'Ordem de compra' existe no card mas o parser "
                        "não conseguiu extrair um valor total dele. Verifique "
                        "se o PDF não está corrompido ou escaneado como imagem."
                    ),
                    severidade=Severidade.ALERTA,
                    dados={"valor_club": str(valor_club)},
                )
            )
        elif card.valor_extraido_pdf is not None:
            delta_pdf = Decimal(valor_club) - Decimal(card.valor_extraido_pdf)
            if abs(delta_pdf) > tol:
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=(
                            f"Club R$ {valor_club} ≠ PDF R$ {card.valor_extraido_pdf} "
                            f"(Δ {delta_pdf:+.2f}, {_classificar(delta_pdf)})"
                        ),
                        descricao=(
                            f"O valor registrado no Club da Cotação "
                            f"(R$ {valor_club}) não bate com o valor extraído "
                            f"do PDF anexado ao card do Pipefy "
                            f"(R$ {card.valor_extraido_pdf}). "
                            f"Diferença: R$ {delta_pdf:+.2f}."
                        ),
                        severidade=Severidade.ERRO,
                        dados={
                            "valor_club": str(valor_club),
                            "valor_pdf": str(card.valor_extraido_pdf),
                            "delta": str(delta_pdf),
                            "categoria": _classificar(delta_pdf),
                        },
                    )
                )

        # ---- Check 4: campo 'Valor' estruturado do card bate? ----
        if card.valor_card is not None:
            delta_card = Decimal(valor_club) - Decimal(card.valor_card)
            if abs(delta_card) > tol:
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=(
                            f"Club R$ {valor_club} ≠ campo Valor do card "
                            f"R$ {card.valor_card} (Δ {delta_card:+.2f})"
                        ),
                        descricao=(
                            f"O valor da OC no Club (R$ {valor_club}) não bate "
                            f"com o campo estruturado 'Valor' do card do Pipefy "
                            f"(R$ {card.valor_card})."
                        ),
                        severidade=Severidade.ERRO,
                        dados={
                            "valor_club": str(valor_club),
                            "valor_card": str(card.valor_card),
                            "delta": str(delta_card),
                        },
                    )
                )

        # ---- Check 5: Cilia (INFO enquanto API real não chega) ----
        cilia = contexto.orcamento_cilia
        if cilia is not None and cilia.encontrado and cilia.valor_total is not None:
            delta_cilia = Decimal(valor_club) - Decimal(cilia.valor_total)
            if abs(delta_cilia) > tol:
                # Cilia hoje é stub → INFO para não poluir o relatório.
                # Quando `CILIA_MODE=http` estiver ativo, essa severidade
                # sobe para ERRO automaticamente via settings.
                sev = (
                    Severidade.ERRO
                    if settings.cilia_mode == "http"
                    else Severidade.INFO
                )
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=(
                            f"Club R$ {valor_club} ≠ Cilia R$ {cilia.valor_total} "
                            f"(Δ {delta_cilia:+.2f}, {_classificar(delta_cilia)})"
                        ),
                        descricao=(
                            f"Divergência entre Club (R$ {valor_club}) e orçamento "
                            f"do Cilia (R$ {cilia.valor_total}). "
                            + (
                                ""
                                if settings.cilia_mode == "http"
                                else "[Cilia em modo STUB — divergência é informativa]"
                            )
                        ),
                        severidade=sev,
                        dados={
                            "valor_club": str(valor_club),
                            "valor_cilia": str(cilia.valor_total),
                            "delta": str(delta_cilia),
                            "cilia_mode": settings.cilia_mode,
                        },
                    )
                )

        return out
