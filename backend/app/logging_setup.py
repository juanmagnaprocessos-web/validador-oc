"""Configuração de logging estruturado com Rich."""
from __future__ import annotations

import logging
from pathlib import Path

from rich.logging import RichHandler

from app.config import BASE_DIR, settings


_configured = False


def setup_logging() -> None:
    global _configured
    if _configured:
        return

    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(exist_ok=True)

    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(rich_tracebacks=True, show_path=False),
            logging.FileHandler(
                logs_dir / "validador.log", encoding="utf-8"
            ),
        ],
    )

    # Reduzir barulho de libs externas
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)
