# Validador OC — Backend

Automação da validação diária de Ordens de Compra da Magna Proteção.
Integra Club da Cotação, Pipefy e Cilia, aplicando as regras R1–R6 e entregando
relatório consolidado ao analista de processos.

## Setup rápido

```bash
cd backend
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# editar .env com credenciais reais
```

## Passos iniciais (uma única vez)

```bash
# 1. Descobrir IDs de fases e campos do Pipefy
python -m scripts.descobrir_ids_pipefy

# 2. (opcional) Capturar fixtures reais de testes
python -m scripts.capturar_fixtures --data 2026-04-05
```

## Uso

### CLI

```bash
# dry-run (não escreve no Pipefy)
python -m app.cli validar --data 2026-04-05 --dry-run

# apply (escreve no Pipefy de verdade)
python -m app.cli validar --data 2026-04-05 --apply
```

### API HTTP

```bash
uvicorn app.main:app --reload --port 8000

# validar um dia
curl -X POST "http://localhost:8000/validar?data=2026-04-05&dry_run=true"

# ver relatório HTML gerado
open http://localhost:8000/relatorio/2026-04-05
```

## Estrutura

```
backend/
├── app/
│   ├── main.py              FastAPI app
│   ├── cli.py               CLI argparse
│   ├── config.py            pydantic-settings
│   ├── db.py                SQLite WAL
│   ├── models.py            Pydantic schemas
│   ├── clients/             Club, Pipefy, Cilia, PDF parser
│   ├── validators/          R1..R6
│   ├── services/            orchestrator, report, emailer
│   └── routers/             validacao, aprovacao, auditoria
├── scripts/
│   ├── descobrir_ids_pipefy.py
│   └── capturar_fixtures.py
├── tests/
│   ├── fixtures/            JSONs de teste
│   └── test_*.py
├── config/                  pipefy_ids.json (gerado)
├── data/                    SQLite db
├── relatorios/              HTML + Excel diários
└── logs/
```

## Regras de validação

| # | Regra | Fontes consultadas |
|---|---|---|
| R1 | Mínimo 3 cotações | Club `/api/getconcorrentescotacao` |
| R2 | Sem duplicidade | Club `/api/getprodutoscotacao` + Pipefy (devolução) + Cilia |
| R3 | Valor consistente | Club `/v3/api/clients/orders/{id}` + PDF Pipefy + Cilia |
| R4 | Placa correta | Normalização + regex + título do card |
| R5 | Fornecedor ativo | `fornecedor.for_status` e `for_excluido` |
| R6 | Data correta (D-1) | `data_pedido` |

## Movimentação Pipefy por forma de pagamento

| Forma | Fase destino |
|---|---|
| A Vista / Cartão | Aguardar Peças |
| Faturado | Aguardar Peças |
| Pix | Programar Pagamento |
| Mercado Livre | Compras Mercado Livre |
| qualquer + divergência | Informações Incorretas |

## Testes

```bash
pytest
pytest -v tests/test_validators.py
```
