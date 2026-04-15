"""Lock global in-process de validações do orchestrator.

Compartilhado entre o endpoint manual `POST /api/validar` e o CRON diário
(`cron_runner.run_daily_validation_job`). Garante que `executar_validacao`
roda serializado dentro do mesmo processo Python — evita colisão em
`backup_db()`, escrita de relatórios no mesmo path, e INSERTs duplicados
em `validacoes` quando user e CRON disparam simultaneamente.

NOTA: este lock é in-memory (asyncio.Lock). Para coordenar entre múltiplas
instâncias do app, a serialização por data_d1 vive em `cron_locks` (DB).
O Render Free roda 1 worker — esse lock é suficiente nesse ambiente.
"""
from __future__ import annotations

import asyncio

_lock = asyncio.Lock()


def get_lock() -> asyncio.Lock:
    return _lock
