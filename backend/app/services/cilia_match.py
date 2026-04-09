"""Cruzamento Club ↔ Cilia: normalização agressiva de descrições + match
fuzzy para decidir se as peças cotadas no Club estão presentes no
orçamento Cilia.

Por que precisa de fuzzy: as descrições não são padronizadas. Exemplos:
  Club  → "PARACHOQUE DIANT INTERNO"
  Cilia → "Parachoque dianteiro int."

Match exato (lowercase) NÃO casa. Sem isso, toda peça vira "sem dados"
no relatório, gerando ruído. Estratégia adotada:

1. **Normalização agressiva**:
   - lowercase
   - remove acentos (`unicodedata.normalize("NFKD")`)
   - remove pontuação (manter só [a-z0-9 ])
   - dedup de espaços
   - strip

2. **Match fuzzy** via `difflib.SequenceMatcher.ratio()` com threshold
   default 0.80. Para cada produto do Club, varre os itens do Cilia e
   pega o melhor score; se >= threshold, conta como match.

3. **Status agregado** retorna um label simples para o relatório:
   - "confirmado": TODOS os produtos do Club encontrados no Cilia
   - "parcial":    PELO MENOS 1 encontrado, mas não todos
   - "sem_dados":  nenhum encontrado, ou Cilia não retornou orçamento

Threshold é configurável; o default 0.80 foi escolhido empiricamente
como bom equilíbrio (`SequenceMatcher` é generoso com prefixos comuns).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable

# Caracteres aceitos após normalização (resto vira espaço)
_RE_NAO_ALFANUM = re.compile(r"[^a-z0-9 ]+")
_RE_ESPACOS = re.compile(r"\s+")


def normalizar_descricao(desc: str | None) -> str:
    """Normaliza uma descrição para comparação fuzzy.

    Exemplo:
        normalizar_descricao("Parachoque DIANT. interno")
        → "parachoque diant interno"
    """
    if not desc:
        return ""
    s = str(desc).lower().strip()
    # Remove acentos: "ç" → "c", "ã" → "a", etc.
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Substitui não-alfanumérico por espaço
    s = _RE_NAO_ALFANUM.sub(" ", s)
    # Dedup espaços
    s = _RE_ESPACOS.sub(" ", s).strip()
    return s


def similaridade(a: str | None, b: str | None) -> float:
    """Score 0..1 entre duas descrições já normalizadas (ou cruas — a
    função normaliza por dentro). Retorna 0 se qualquer lado for vazio."""
    na = normalizar_descricao(a)
    nb = normalizar_descricao(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


@dataclass
class MatchPeca:
    """Resultado do match de uma peça do Club contra os itens do Cilia."""
    descricao_club: str
    encontrada: bool
    melhor_match_cilia: str | None
    score: float


@dataclass
class ResultadoMatch:
    """Resultado agregado do cruzamento de uma OC vs orçamento Cilia."""
    status: str            # "confirmado" | "parcial" | "sem_dados"
    match_count: int       # quantas peças do Club bateram
    total_count: int       # total de peças do Club
    detalhes: list[MatchPeca]

    @property
    def percent(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.match_count / self.total_count


def _extrair_descricoes(itens: Iterable[Any]) -> list[tuple[str, Any]]:
    """Aceita lista de objetos (Pydantic com `.descricao`) ou dicts."""
    out: list[tuple[str, Any]] = []
    for it in itens or []:
        if hasattr(it, "descricao"):
            desc = getattr(it, "descricao", None)
        elif isinstance(it, dict):
            desc = it.get("descricao") or it.get("description")
        else:
            desc = None
        if desc:
            out.append((str(desc), it))
    return out


def match_pecas(
    produtos_club: Iterable[Any],
    itens_cilia: Iterable[Any],
    *,
    threshold: float = 0.80,
) -> ResultadoMatch:
    """Cruza a lista de produtos do Club com a lista de itens do Cilia.

    Para cada produto do Club, busca o item Cilia com maior similaridade
    e marca como encontrado se o score >= threshold.

    Retorna `ResultadoMatch` com status agregado.
    """
    club_pares = _extrair_descricoes(produtos_club)
    cilia_pares = _extrair_descricoes(itens_cilia)

    # Sem dados de Cilia → status "sem_dados", mas total_count vem do Club
    if not cilia_pares:
        return ResultadoMatch(
            status="sem_dados",
            match_count=0,
            total_count=len(club_pares),
            detalhes=[
                MatchPeca(
                    descricao_club=desc, encontrada=False,
                    melhor_match_cilia=None, score=0.0,
                )
                for desc, _ in club_pares
            ],
        )

    # Pré-normaliza descrições do Cilia uma vez (otimização)
    cilia_norm = [
        (normalizar_descricao(desc), desc) for desc, _ in cilia_pares
    ]

    detalhes: list[MatchPeca] = []
    match_count = 0
    for desc_club, _ in club_pares:
        norm_club = normalizar_descricao(desc_club)
        if not norm_club:
            detalhes.append(MatchPeca(
                descricao_club=desc_club, encontrada=False,
                melhor_match_cilia=None, score=0.0,
            ))
            continue
        melhor_score = 0.0
        melhor_desc: str | None = None
        for norm_cilia, desc_cilia in cilia_norm:
            if not norm_cilia:
                continue
            s = SequenceMatcher(None, norm_club, norm_cilia).ratio()
            if s > melhor_score:
                melhor_score = s
                melhor_desc = desc_cilia
        encontrada = melhor_score >= threshold
        if encontrada:
            match_count += 1
        detalhes.append(MatchPeca(
            descricao_club=desc_club,
            encontrada=encontrada,
            melhor_match_cilia=melhor_desc,
            score=round(melhor_score, 3),
        ))

    total = len(club_pares)
    if total == 0:
        status = "sem_dados"
    elif match_count == 0:
        status = "sem_dados"
    elif match_count == total:
        status = "confirmado"
    else:
        status = "parcial"

    return ResultadoMatch(
        status=status,
        match_count=match_count,
        total_count=total,
        detalhes=detalhes,
    )
