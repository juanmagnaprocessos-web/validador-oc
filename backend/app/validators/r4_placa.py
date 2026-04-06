"""R4 — Placa sem erro de digitação.

- Campo `identificador` vem com hífen (ex: "PQX-2I72")
- Normalização: .replace("-", "").upper().strip()
- Valida contra regex Mercosul (AAA0A00) ou antigo (AAA0000)
- Cruza com título do card Pipefy (que é a placa sem hífen, ex: "PAN1D24")
"""
from __future__ import annotations

import re

from app.models import ContextoValidacao, Divergencia, Severidade
from app.validators.base import Regra


RE_MERCOSUL = re.compile(r"^[A-Z]{3}\d[A-Z]\d{2}$")
RE_ANTIGO = re.compile(r"^[A-Z]{3}\d{4}$")


class R4Placa(Regra):
    codigo = "R4"
    nome = "Placa sem erro de digitação"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        oc = contexto.oc
        placa = oc.placa_normalizada

        out: list[Divergencia] = []

        if not placa:
            return [
                Divergencia(
                    regra=self.codigo,
                    titulo="Placa ausente",
                    descricao="OC sem identificador (placa) — R4 falhou.",
                    severidade=Severidade.ERRO,
                )
            ]

        # Formato
        if not (RE_MERCOSUL.match(placa) or RE_ANTIGO.match(placa)):
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Placa '{placa}' em formato inválido",
                    descricao=(
                        f"A placa '{oc.identificador}' (normalizada: '{placa}') "
                        "não corresponde aos formatos brasileiros "
                        "(Mercosul AAA0A00 ou antigo AAA0000)."
                    ),
                    severidade=Severidade.ERRO,
                    dados={"placa_original": oc.identificador, "normalizada": placa},
                )
            )

        # Cruzamento com título do card
        if contexto.card_pipefy and contexto.card_pipefy.title:
            titulo_card = (
                contexto.card_pipefy.title.replace("-", "")
                .upper()
                .strip()
            )
            if titulo_card and titulo_card != placa:
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=f"Placa OC ({placa}) ≠ card Pipefy ({titulo_card})",
                        descricao=(
                            f"A placa do Club ({placa}) não bate com o título do "
                            f"card no Pipefy ({titulo_card}). Possível erro de "
                            "digitação/cadastro."
                        ),
                        severidade=Severidade.ERRO,
                        dados={"placa_club": placa, "placa_pipefy": titulo_card},
                    )
                )

        return out
