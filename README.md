# Validador OC — Magna Proteção

Automação da validação diária de Ordens de Compra (OCs) geradas no Club da
Cotação. Substitui ~4 horas/dia de trabalho manual do analista de processos
cruzando dados entre Club, Cilia e Pipefy, aplicando 6 regras de validação e
atualizando os cards do pipe "Sinistro — Logística".

## Status

Backend completo com todas as 6 regras (R1–R6), integração real com Club da
Cotação e Pipefy, stub do Cilia (aguardando credenciais) e dashboard React
para revisão pelo analista.

- Backend: FastAPI + httpx + SQLite (WAL) + pdfplumber + openpyxl
- Frontend: React 18 + Vite + TypeScript
- 36/36 testes passando
- Regra Mercado Livre e detecção retroativa de cards já processados
  implementadas

## Arquitetura

```
validador-oc/
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI app + rotas
│   │   ├── config.py               # pydantic-settings (.env)
│   │   ├── db.py                   # SQLite WAL + migrações idempotentes
│   │   ├── models.py               # Pydantic: OC, Card, Resultado, Status
│   │   ├── logging_setup.py
│   │   ├── clients/
│   │   │   ├── club_client.py      # Club da Cotação (JWT + refresh)
│   │   │   ├── pipefy_client.py    # Pipefy GraphQL
│   │   │   ├── cilia_client.py     # Interface + stub
│   │   │   └── pdf_parser.py       # Extração de valor de PDF (pdfplumber)
│   │   ├── validators/             # R1..R6 — uma regra por arquivo
│   │   ├── services/
│   │   │   ├── orchestrator.py     # Pipeline coleta→valida→persiste→atua
│   │   │   ├── report.py           # Relatório HTML + Excel
│   │   │   ├── emailer.py          # Notificação SMTP (template Jinja2)
│   │   │   └── compradores.py      # Tabela auxiliar created_by → nome/email
│   │   └── routers/validacao.py    # POST /validar, GET /historico, etc.
│   ├── templates/                  # Jinja2 (relatório HTML, e-mail)
│   ├── tests/                      # pytest (36 testes)
│   ├── scripts/
│   │   ├── descobrir_ids_pipefy.py # Introspecção GraphQL do pipe
│   │   └── capturar_fixtures.py    # Salva respostas reais para testes
│   ├── config/                     # pipefy_ids.json (gerado, não commitado)
│   ├── data/                       # validador.db (gerado, não commitado)
│   ├── relatorios/                 # HTML+XLSX por dia (gerado)
│   ├── requirements.txt
│   └── .env.example
└── frontend/
    ├── src/
    │   ├── App.tsx
    │   ├── api/client.ts            # fetch helpers + tipos
    │   └── components/
    │       ├── Cards.tsx            # Resumo (6 contadores)
    │       └── ResultadosTable.tsx
    ├── index.html
    ├── package.json
    └── vite.config.ts              # Proxy /api → localhost:8000
```

## Regras implementadas

| Regra | Descrição |
|-------|-----------|
| R1 | Mínimo de 3 cotações concorrentes |
| R2 | Duplicidade de peça entre fornecedores (cruza `getprodutoscotacao`) |
| R3 | Conferência de valor: Club × PDF Pipefy × Cilia (tolerância configurável) |
| R4 | Placa normalizada e compatível com o título do card no Pipefy |
| R5 | Fornecedor ativo (`for_status=1`, `for_excluido=0`) |
| R6 | Data do pedido = D-1 |

## Estados de validação

Cada OC é classificada em um dos cinco estados:

| Estado | Cor | Ação no Pipefy |
|--------|-----|----------------|
| `aprovada` | verde | Move para "Programar Pagamento" / "Aguardar Peças" conforme forma |
| `divergencia` | laranja | Move para "Informações Incorretas" com justificativa |
| `bloqueada` | vermelho | Falha grave de coleta |
| `aguardando_ml` | amarelo | **Não move** — fornecedor Mercado Livre requer validação manual do analista |
| `ja_processada` | cinza | **Não toca** — card já estava fora da fase "Validação" (processado por humano ou execução anterior) |

## Fonte da verdade para "já processada"

O orquestrador varre cards em 5 fases do Pipefy (Validação, Aguardar Peças,
Programar Pagamento, Compras ML e Informações Incorretas) e indexa por placa.
Se o card de uma OC estiver em qualquer fase diferente de "Validação", o
validador marca como `ja_processada` e **não reprocessa** — evita falsas
divergências em datas retroativas e respeita ações manuais do analista.

## Setup

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate         # Windows
# source .venv/bin/activate    # Linux/Mac
pip install -r requirements.txt

cp .env.example .env           # e preencha com credenciais reais

# Descobrir IDs do pipe (gera config/pipefy_ids.json):
python -m scripts.descobrir_ids_pipefy

# Rodar testes:
pytest

# Subir API:
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Endpoints principais:
- `POST /validar?data=YYYY-MM-DD&dry_run=true` — executa pipeline
- `GET /historico` — últimas execuções
- `GET /validacoes/{id}/resultados` — detalhe de uma execução
- `GET /relatorio/{data}` — relatório HTML gerado
- `GET /relatorio/{data}/excel` — Excel gerado
- `GET /docs` — Swagger UI

### Frontend

```bash
cd frontend
npm install
npm run dev      # http://localhost:5174
```

O Vite faz proxy de `/api/*` para `http://localhost:8000/*`, então basta o
backend estar rodando na porta 8000.

## CLI

```bash
cd backend
python -m app.cli validar --data 2026-04-05 --dry-run
python -m app.cli validar --data 2026-04-05 --apply      # aplica no Pipefy
```

## Dry-run vs apply

Por padrão tudo roda em `dry_run=True`: o SQLite é populado, os relatórios
são gerados, mas nenhuma mutation é enviada ao Pipefy. Só passa a mexer no
Pipefy com `--apply` na CLI ou `dry_run=false` no endpoint.

## Stack e decisões

- **SQLite raw, sem ORM** — mesmo padrão do projeto Gestão POP da Magna
- **httpx** para todos os clients HTTP (assíncrono)
- **Migrations aditivas** via `PRAGMA table_info` + `ALTER TABLE` (idempotente)
- **Cilia em modo stub** enquanto credenciais não chegam. Swap para
  `CiliaHTTPClient` sem refatoração quando a API estiver disponível
  (flag `CILIA_MODE=stub|http` no `.env`).
- **Tag "ML" só no sistema local** (dashboard + HTML + Excel). Não mexe em
  labels nativas do Pipefy por decisão de produto.

## Segurança

- `.env` nunca commitado
- `config/pipefy_ids.json` ignorado (contém mapeamento do pipe)
- `data/*.db` e `relatorios/*` ignorados (dados reais de OCs)
- `backend/.env.example` usa placeholders — **nunca** commitar credenciais
  reais nesse arquivo

## Roadmap

- Integração HTTP real do Cilia quando as credenciais chegarem
- Scheduler diário (APScheduler) para execução automática
- Deploy em servidor interno da Magna via Docker
- Ajustes finos de tolerância e regras após primeiras semanas em produção
