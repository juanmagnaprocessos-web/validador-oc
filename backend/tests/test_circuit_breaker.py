"""Testes do CircuitBreaker — especialmente o comportamento de
`ignored_excs`, que garante que erros do CLIENTE (ex: 404, parametro
invalido) nao consumam o contador de falhas destinado a proteger contra
falhas do SERVIDOR.
"""
from __future__ import annotations

import pytest

from app.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpen,
    CircuitState,
)


class _NotFoundError(Exception):
    """Exemplo de exception de erro-de-cliente (nao deve contar)."""


class _ServerError(Exception):
    """Exemplo de exception de erro-de-servidor (deve contar)."""


@pytest.mark.asyncio
async def test_ignored_excs_nao_contabilizam_falha():
    """Exceções em `ignored_excs` devem ser propagadas sem abrir o breaker."""
    breaker = CircuitBreaker(
        "teste",
        fail_threshold=3,
        ignored_excs=(_NotFoundError,),
    )

    async def sempre_404():
        raise _NotFoundError("simulado 404")

    # 10 chamadas com NotFoundError — breaker deve permanecer CLOSED
    for _ in range(10):
        with pytest.raises(_NotFoundError):
            await breaker.call(sempre_404)

    assert breaker.state == CircuitState.CLOSED
    assert breaker.failures == 0


@pytest.mark.asyncio
async def test_excecoes_nao_ignoradas_ainda_abrem_breaker():
    """Exceções fora de `ignored_excs` continuam contando e abrem o breaker."""
    breaker = CircuitBreaker(
        "teste",
        fail_threshold=3,
        ignored_excs=(_NotFoundError,),
    )

    async def sempre_erro_servidor():
        raise _ServerError("simulado 500")

    # 3 falhas -> breaker abre
    for _ in range(3):
        with pytest.raises(_ServerError):
            await breaker.call(sempre_erro_servidor)

    assert breaker.state == CircuitState.OPEN

    # Proxima chamada rejeitada sem executar func
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(sempre_erro_servidor)


@pytest.mark.asyncio
async def test_mistura_ignorada_e_contabilizada():
    """Exceções ignoradas NAO devem zerar nem incrementar o contador
    de falhas; apenas exceções nao-ignoradas afetam o estado."""
    breaker = CircuitBreaker(
        "teste",
        fail_threshold=3,
        ignored_excs=(_NotFoundError,),
    )

    async def erro_404():
        raise _NotFoundError()

    async def erro_500():
        raise _ServerError()

    # 2 falhas de servidor
    for _ in range(2):
        with pytest.raises(_ServerError):
            await breaker.call(erro_500)
    assert breaker.failures == 2

    # 5 erros 404 no meio — nao devem zerar nem incrementar
    for _ in range(5):
        with pytest.raises(_NotFoundError):
            await breaker.call(erro_404)
    assert breaker.failures == 2
    assert breaker.state == CircuitState.CLOSED

    # 1 ultima falha de servidor atinge o threshold
    with pytest.raises(_ServerError):
        await breaker.call(erro_500)
    assert breaker.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_default_sem_ignored_excs_mantem_comportamento_legado():
    """Sem configurar `ignored_excs`, qualquer exception conta — garante
    compatibilidade com o comportamento original."""
    breaker = CircuitBreaker("teste", fail_threshold=2)

    async def erro():
        raise RuntimeError("qualquer coisa")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(erro)

    assert breaker.state == CircuitState.OPEN
