#!/usr/bin/env bash
set -euo pipefail

echo "=== Instalando dependencias backend ==="
cd backend
pip install -r requirements.txt

echo "=== Buildando frontend ==="
cd ../frontend
npm install
npm run build

echo "=== Build concluido ==="
ls -la dist/
