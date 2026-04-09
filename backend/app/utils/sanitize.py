"""Utilitarios de sanitizacao para logs e registros de auditoria."""
from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# Nomes de query params que devem ser mascarados antes de persistir em log/DB.
_SENSITIVE_PARAMS = frozenset({
    "token",
    "password",
    "api_key",
    "secret",
    "auth",
    "senha",
    "authorization",
})


def sanitizar_url(url: str) -> str:
    """Remove valores de query params sensiveis de uma URL.

    Params cujo nome (case-insensitive) esteja em ``_SENSITIVE_PARAMS``
    tem o valor substituido por ``***``.  A estrutura da URL e preservada.

    Uso recomendado: chamar ANTES de salvar a URL em ``registrar_chamada_api``
    (ver ``app/db.py``).

    Exemplo::

        >>> sanitizar_url("https://api.example.com/v1?token=abc123&page=1")
        'https://api.example.com/v1?token=***&page=1'
    """
    parsed = urlparse(url)
    if not parsed.query:
        return url

    qs = parse_qs(parsed.query, keep_blank_values=True)
    sanitized: dict[str, list[str]] = {}
    for key, values in qs.items():
        if key.lower() in _SENSITIVE_PARAMS:
            sanitized[key] = ["***"] * len(values)
        else:
            sanitized[key] = values

    clean_query = urlencode(sanitized, doseq=True)
    return urlunparse(parsed._replace(query=clean_query))
