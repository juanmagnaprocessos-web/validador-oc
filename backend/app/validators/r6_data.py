"""R6 — Data correta (D-1).

Apenas OCs do dia anterior são validadas. OCs com data diferente de D-1
são bloqueadas, exceto em casos reportados pelo analista.
"""
from __future__ import annotations

from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


class R6Data(Regra):
    codigo = "R6"
    nome = "Data correta (D-1)"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        oc = contexto.oc
        data_oc = oc.data_pedido
        esperada = contexto.data_d1

        if data_oc is None:
            # Data ausente não bloqueia sozinha — só vira alerta.
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo="Data do pedido ausente",
                    descricao=(
                        "OC sem data_pedido — não foi possível confirmar "
                        "se corresponde a D-1."
                    ),
                    severidade=Severidade.ALERTA,
                )
            ]

        if data_oc != esperada:
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo=(
                        f"Data {data_oc.isoformat()} ≠ D-1 ({esperada.isoformat()})"
                    ),
                    descricao=(
                        f"A OC tem data {data_oc.isoformat()}, mas a validação "
                        f"é para {esperada.isoformat()} (D-1). Verificar se a OC "
                        "pertence ao lote do dia."
                    ),
                    severidade=Severidade.ERRO,
                    dados={
                        "data_oc": data_oc.isoformat(),
                        "data_d1": esperada.isoformat(),
                    },
                )
            ]

        return []
