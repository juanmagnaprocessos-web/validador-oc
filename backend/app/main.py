"""FastAPI app principal do validador-oc."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import init_db
from app.logging_setup import setup_logging
from app.routers import admin, validacao

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Validador OC — Magna Proteção",
    description=(
        "Automação da validação diária de Ordens de Compra. "
        "Integra Club da Cotação, Pipefy e Cilia, aplicando regras R1–R6."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(validacao.router)
app.include_router(admin.auth_router)
app.include_router(admin.admin_router)


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
