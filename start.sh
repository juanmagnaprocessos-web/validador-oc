#!/usr/bin/env bash
set -euo pipefail

cd backend

echo "=== Iniciando Validador OC ==="
echo "Python: $(python --version)"
echo "DB_PATH: ${DB_PATH:-data/validador.db}"
echo "APP_ENV: ${APP_ENV:-development}"

exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
