"""FastAPI app principal do validador-oc."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import init_db
from app.logging_setup import setup_logging
from app.routers import admin, validacao

setup_logging()

# --- Docs condicionais: desabilitadas fora de development ---
_is_dev = settings.app_env == "development"


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
    docs_url="/docs" if _is_dev else None,
    redoc_url="/redoc" if _is_dev else None,
    openapi_url="/openapi.json" if _is_dev else None,
)


# --- Security Headers Middleware ---
@app.middleware("http")
async def security_headers_middleware(request: Request, call_next) -> Response:
    response: Response = await call_next(request)
    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = (
        "geolocation=(), microphone=(), camera=()"
    )
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self'; "
        "connect-src 'self'; "
        "frame-ancestors 'none'"
    )
    return response


# --- CORS (origens via settings, métodos e headers explícitos) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
)

app.include_router(validacao.router)
app.include_router(admin.auth_router)
app.include_router(admin.admin_router)


@app.get("/")
async def root():
    info: dict[str, str] = {
        "app": "validador-oc",
        "version": "0.1.0",
    }
    if _is_dev:
        info["docs"] = "/docs"
    return info


@app.get("/health")
async def health():
    return {"status": "ok"}
