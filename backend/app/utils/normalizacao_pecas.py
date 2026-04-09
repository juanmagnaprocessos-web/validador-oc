"""Normalização e comparação de descrições de peças automotivas.

Usado pela R2 cross-time para correlacionar peças entre Club (OC) e
Pipefy (devolução/cancelamento), onde as descrições variam livremente.
"""
from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher

# Abreviações comuns no domínio automotivo (expandidas para forma canônica)
_ABREVIACOES: list[tuple[str, str]] = [
    (r"\bESQ\b", "ESQUERDO"),
    (r"\bDIR\b", "DIREITO"),
    (r"\bDIANT\b", "DIANTEIRO"),
    (r"\bTRAS\b", "TRASEIRO"),
    (r"\bALOJ\b", "ALOJAMENTO"),
    (r"\bSUP\b", "SUPERIOR"),
    (r"\bINF\b", "INFERIOR"),
    (r"\bFRONT\b", "FRONTAL"),
    (r"\bLAT\b", "LATERAL"),
    (r"\bRAD\b", "RADIADOR"),
    (r"\bPARAC\b", "PARACHOQUE"),
    (r"\bPARABR\b", "PARABRISA"),
    (r"\bRET\b", "RETROVISOR"),
    (r"\bCJ\b", "CONJUNTO"),
]

# Palavras de ligação que podem ser removidas sem perda semântica
_STOPWORDS = {"DE", "DO", "DA", "DOS", "DAS", "O", "A", "OS", "AS", "E", "P/", "P"}


def normalizar_descricao(desc: str | None) -> str:
    """Normaliza descrição de peça para comparação tolerante.

    - Remove acentos
    - Uppercase
    - Expande abreviações comuns
    - Remove stopwords
    - Remove caracteres especiais
    - Colapsa espaços
    """
    if not desc:
        return ""
    # Remove acentos
    s = unicodedata.normalize("NFKD", desc)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper().strip()
    # Remove pontuação exceto hífen (usado em peças compostas)
    s = re.sub(r"[^\w\s-]", " ", s)
    # Expande abreviações
    for padrao, expansao in _ABREVIACOES:
        s = re.sub(padrao, expansao, s)
    # Remove stopwords
    tokens = s.split()
    tokens = [t for t in tokens if t not in _STOPWORDS]
    # Colapsa espaços
    return " ".join(tokens)


def descricoes_similares(desc_a: str | None, desc_b: str | None) -> float:
    """Retorna score de similaridade (0.0-1.0) entre duas descrições.

    Ambas são normalizadas antes da comparação. Usa SequenceMatcher
    (difflib) que é eficiente para strings curtas como nomes de peças.

    Se `desc_b` contém múltiplas peças (separadas por \\n, vírgula ou ;),
    verifica se `desc_a` casa com ALGUMA delas (retorna o maior score).
    """
    na = normalizar_descricao(desc_a)
    if not na or not desc_b:
        return 0.0

    # Dividir desc_b em sub-peças se multi-linha
    separadores = re.compile(r"[\n;,]+")
    partes_b = [p.strip() for p in separadores.split(desc_b) if p.strip()]

    if len(partes_b) <= 1:
        # Comparação simples
        nb = normalizar_descricao(desc_b)
        if not nb:
            return 0.0
        if na == nb:
            return 1.0
        return SequenceMatcher(None, na, nb).ratio()

    # Multi-peça: retornar o MAIOR score entre as partes
    melhor = 0.0
    for parte in partes_b:
        np = normalizar_descricao(parte)
        if not np:
            continue
        if na == np:
            return 1.0
        score = SequenceMatcher(None, na, np).ratio()
        if score > melhor:
            melhor = score
    return melhor


# Threshold padrão para considerar match
THRESHOLD_MATCH = 0.7
