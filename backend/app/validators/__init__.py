"""Regras de validação R1–R6."""
from app.validators.base import Regra, aplicar_regras
from app.validators.r1_minimo_cotacoes import R1MinimoCotacoes
from app.validators.r2_duplicidade import R2Duplicidade
from app.validators.r3_valor import R3Valor
from app.validators.r4_placa import R4Placa
from app.validators.r5_fornecedor import R5Fornecedor
from app.validators.r6_data import R6Data

REGRAS_PADRAO: list[Regra] = [
    R6Data(),
    R5Fornecedor(),
    R4Placa(),
    R1MinimoCotacoes(),
    R2Duplicidade(),
    R3Valor(),
]

__all__ = [
    "Regra",
    "aplicar_regras",
    "REGRAS_PADRAO",
    "R1MinimoCotacoes",
    "R2Duplicidade",
    "R3Valor",
    "R4Placa",
    "R5Fornecedor",
    "R6Data",
]
