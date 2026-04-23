"""Circuit breaker leve — sem dependencias externas.

Protege chamadas a APIs externas contra falhas em cascata.
Quando o numero de falhas consecutivas atinge `fail_threshold`,
o circuito abre e rejeita chamadas por `reset_timeout` segundos.
Apos esse periodo, entra em HALF_OPEN e permite uma tentativa:
  - se sucesso, volta a CLOSED
  - se falha, reabre
"""
from __future__ import annotations

import time
from enum import Enum


class CircuitState(Enum):
    CLOSED = "closed"       # Normal — chamadas permitidas
    OPEN = "open"           # Bloqueado — rejeita chamadas
    HALF_OPEN = "half_open"  # Testando — permite 1 tentativa


class CircuitBreakerOpen(Exception):
    """Levantada quando o circuit breaker esta aberto."""


class CircuitBreaker:
    """Circuit breaker in-process, thread-safe o suficiente para asyncio
    single-thread (event loop unico).

    Parametro `ignored_excs`: tupla de excecoes que devem ser propagadas
    SEM contar como falha (ex: 404 / erro de cliente — nao sinaliza que
    a API esta fora do ar, apenas que o recurso nao existe).
    """

    def __init__(
        self,
        name: str,
        fail_threshold: int = 5,
        reset_timeout: float = 60.0,
        ignored_excs: tuple[type[BaseException], ...] = (),
    ) -> None:
        self.name = name
        self.fail_threshold = fail_threshold
        self.reset_timeout = reset_timeout
        self.ignored_excs = ignored_excs
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.last_failure_time = 0.0

    async def call(self, func, *args, **kwargs):
        """Executa `func` protegida pelo circuit breaker.

        Se o circuito estiver OPEN e o timeout de reset nao expirou,
        levanta `CircuitBreakerOpen` sem chamar `func`.
        """
        if self.state == CircuitState.OPEN:
            if time.monotonic() - self.last_failure_time > self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise CircuitBreakerOpen(
                    f"Circuit breaker '{self.name}' esta aberto "
                    f"(falhas={self.failures}, aguardando reset)"
                )

        try:
            result = await func(*args, **kwargs)
            # Sucesso: reseta falhas e volta ao normal
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
            self.failures = 0
            return result
        except self.ignored_excs:
            # Erro de cliente (ex: 404) — propaga sem contar
            raise
        except Exception:
            self.failures += 1
            self.last_failure_time = time.monotonic()
            if self.failures >= self.fail_threshold:
                self.state = CircuitState.OPEN
            raise
