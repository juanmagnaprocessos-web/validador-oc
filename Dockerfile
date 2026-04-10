FROM python:3.12-slim

WORKDIR /app

# Dependencias do sistema (pdfplumber precisa de gcc + curl para healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar requirements Python
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar codigo do backend
COPY backend/ /app/backend/

# Criar usuario nao-root e dar permissao aos diretorios
RUN useradd -m -u 1000 appuser && \
    mkdir -p /data && \
    chown -R appuser:appuser /app /data

USER appuser

# Working dir para o app
WORKDIR /app/backend

EXPOSE 8080

# Healthcheck para Fly.io saber se a app esta viva
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
