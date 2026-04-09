"""FastAPI app principal do validador-oc."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import init_db
from app.logging_setup import setup_logging
from app.routers import admin, validacao

setup_logging()

# --- Docs condicionais: desabilitadas fora de development ---
_is_dev = settings.app_env == "development"

# --- Frontend estático (build do Vite) ---
_FRONTEND_DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"


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
    docs_url="/api/docs" if _is_dev else None,
    redoc_url="/api/redoc" if _is_dev else None,
    openapi_url="/api/openapi.json" if _is_dev else None,
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

app.include_router(validacao.router, prefix="/api")
app.include_router(admin.auth_router, prefix="/api")
app.include_router(admin.admin_router, prefix="/api")


@app.get("/api")
async def api_root():
    info: dict[str, str] = {
        "app": "validador-oc",
        "version": "0.1.0",
    }
    if _is_dev:
        info["docs"] = "/api/docs"
    return info


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/setup-seed")
async def force_seed():
    """Endpoint temporario para forcar seed de usuarios no deploy."""
    from app.db import get_conn, _seed_usuarios
    with get_conn() as conn:
        # Forcar seed mesmo se tabela existe mas esta vazia
        row = conn.execute("SELECT COUNT(*) AS n FROM usuarios").fetchone()
        if row["n"] > 0:
            return {"status": "usuarios ja existem", "count": row["n"]}
        _seed_usuarios(conn)
        conn.commit()
    return {"status": "seed executado com sucesso"}


# --- Servir frontend estático em produção ---
# Em dev, o Vite cuida disso. Em produção, o FastAPI serve o dist/.
if _FRONTEND_DIST.is_dir():
    # Assets (JS, CSS, imagens) — cache longo
    app.mount(
        "/assets",
        StaticFiles(directory=_FRONTEND_DIST / "assets"),
        name="assets",
    )

    # Qualquer outra rota não-API → retorna index.html (SPA fallback)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        from fastapi.responses import FileResponse

        file_path = _FRONTEND_DIST / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(_FRONTEND_DIST / "index.html")
