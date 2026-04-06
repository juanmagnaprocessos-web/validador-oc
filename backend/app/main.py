"""FastAPI app principal do validador-oc."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import init_db
from app.logging_setup import setup_logging
from app.routers import validacao

setup_logging()

app = FastAPI(
    title="Validador OC — Magna Proteção",
    description=(
        "Automação da validação diária de Ordens de Compra. "
        "Integra Club da Cotação, Pipefy e Cilia, aplicando regras R1–R6."
    ),
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restringir em produção
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(validacao.router)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


@app.get("/")
async def root():
    return {
        "app": "validador-oc",
        "version": "0.1.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
