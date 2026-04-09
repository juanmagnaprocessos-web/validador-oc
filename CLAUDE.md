# Validador OC — Magna Protecao

## Sobre o usuario

Juan Pablo trabalha na Magna Protecao Automotiva, area de Processos. Responsavel pelo mapeamento de POPs e validacao de Ordens de Compra. Idioma: Portugues (BR).

**Background tecnico**: Nao tem familiaridade com termos de infra/DevOps. Explicar conceitos com analogias simples ANTES de mostrar comandos. Compreende bem quando tem o "porque" antes do "como".

**Decisoes**: Decisivo com opcoes claras + trade-offs. Aceita risco consciente quando ve beneficio pratico. Prefere recomendacao explicita.

## Preferencias de trabalho

- **Agentes paralelos**: Sempre usar agentes multiplos em tarefas complexas + 1 agente "advogado do diabo" que questione falhas.
- **Questionar antes de agir**: Perguntar antes de mudancas significativas na arquitetura ou funcionalidades. Para bugs simples, pode agir direto.
- **Autonomia**: Carta branca para executar comandos sem pedir aprovacao individual, mas informar o plano antes de iniciar series de acoes.
- **Idioma**: Sempre responder em Portugues.

## O que e este projeto

Automacao que substitui 4h/dia de trabalho manual validando 30-50 Ordens de Compra diarias. Coleta OCs do D-1 no Club da Cotacao, cruza com Pipefy e Cilia, aplica 6 regras (R1-R6), e entrega dashboard + relatorios ao analista.

## Stack

- **Backend**: Python 3.12 + FastAPI + httpx + pydantic-settings + SQLite WAL + pdfplumber + openpyxl + Jinja2 + tenacity
- **Frontend**: React 18 + Vite 5 + TypeScript (CSS inline, sem framework UI)
- **Auth**: HTTP Basic Auth + bcrypt (sem JWT)
- **GitHub**: https://github.com/juanmagnaprocessos-web/validador-oc (privado, branch `main`)

## Credenciais e IDs

- **Club da Cotacao**: `processo2@magnaprotecao.com.br` / `Senha2020@` — JWT em `/v3/api/auth`
- **Pipefy**: token pessoal no `.env`. Pipe principal: **305587531** (SINISTRO - LOGISTICA). Pipe devolucao: **305658860**
- **Cilia**: reCAPTCHA Enterprise bloqueia automacao. Modo `deeplink` (link clicavel no relatorio). CiliaHTTPClient mantido como codigo morto.
- **Login dashboard**: `juanpablo` / `admin123` ou `admin` / `admin123`

## Comandos essenciais

```bash
# Subir backend
cd backend && .venv\Scripts\activate && uvicorn app.main:app --port 8000

# Subir frontend
cd frontend && npm run dev -- --host 127.0.0.1 --port 5174

# URLs
# Dashboard: http://127.0.0.1:5174
# API Swagger: http://127.0.0.1:8000/docs

# Validacao via CLI
python -m app.cli validar --data 2026-04-08 --dry-run

# Descobrir IDs do Pipefy
python -m scripts.descobrir_ids_pipefy
python -m scripts.descobrir_ids_pipefy --pipe-id 305658860 --output config/pipefy_ids_devolucao.json
```

## Regras R1-R6

- **R1**: min 3 cotacoes
- **R2**: sem duplicidade de pecas (intra-cotacao + cross-time 90d). Correlacao por n_oc + descricao normalizada. Verifica devolucao POR PECA.
- **R3**: 5 checks (card existe, anexo OC, PDF parseavel, valor Club=PDF, valor Club=card)
- **R4**: placa normalizada + regex Mercosul/antigo
- **R5**: fornecedor ativo e nao excluido
- **R6**: data_pedido == D-1

## Funcionalidades implementadas (sessoes 9-10)

### Correlacao por peca especifica
- R2 cross-time busca devolucao pelo `n_oc` da OC anterior (nao so placa)
- `app/utils/normalizacao_pecas.py`: normaliza descricoes (abreviacoes ESQ->ESQUERDO, acentos, multi-linha)
- Match com threshold 0.7 (SequenceMatcher)
- Labels: `sim_com_devolucao_peca`, `sim_devolucao_outra_peca`, `sim_sem_devolucao`, `sim_sem_devolucao_mesmo_forn`

### Relatorio HTML por peca
- Cada peca duplicada mostra: OC anterior + link, status devolucao (check/alerta/sem), link card devolucao
- Bloco "Outras duplicidades desta placa (90d)" para pecas NAO na OC atual
- Excel com mesmos detalhes

### Pipe de devolucao (305658860)
- Start form fields: placa, n_oc, cite_as_pecas, cod, motivo_devolucao, valor_da_peca, fornecedor, data_limite
- 8 fases: Verificar Possibilidade -> Devolucao ML -> Providenciar Recolha -> Peca em Estoque -> Conciliacao -> Concluido / Peca Nao Localizada / Cancelado
- Cache SQLite indexado por placa + n_oc

### Cancelamentos
- Fases: "Informacoes Incorretas" (334019348) + "Cancelados" (337982176) do pipe principal
- Cache com descricao_pecas e codigo_oc

## Pendencias conhecidas

1. **Cilia API**: reCAPTCHA bloqueia. CiliaHTTPClient pronto, trocar `CILIA_MODE=http` quando abrir API
2. **SMTP**: configurar no `.env` quando autorizado
3. **Deploy**: decidido servidor interno, falta Dockerfile
4. **Scheduler**: APScheduler opcional, usuario prefere botao manual
5. **Parser PDF**: pode precisar ajuste para variacoes de formatacao

## Arquivos-chave

- `backend/app/services/orchestrator.py` — pipeline principal
- `backend/app/validators/r2_duplicidade.py` — R2 cross-time com correlacao por peca
- `backend/app/utils/normalizacao_pecas.py` — normalizacao de descricoes
- `backend/app/db.py` — SQLite (historico, caches devolucao/cancelamento)
- `backend/app/clients/{club,pipefy,cilia}_client.py` — clientes externos
- `backend/app/models.py` — dominio (OrdemCompra, ResultadoValidacao, etc.)
- `backend/templates/relatorio.html.j2` — template do relatorio
- `backend/config/pipefy_ids.json` — IDs de fases/campos (gerado pelo script)

## Historico de sessoes

- **Sessao 6-7** (2026-04-01): Cilia pesquisa, Miro integrado, debug profundo sub-etapas
- **Sessao 8** (2026-04-07): Deploy VM simplificado (scripts instalar/iniciar/atualizar)
- **Sessao 9** (2026-04-07/08): Cilia deeplink, cancelamentos, plano enriquecimento relatorio
- **Sessao 10** (2026-04-09): RED TEAM 3 agentes, correlacao por peca (n_oc + descricao normalizada), historico duplicidades placa, template HTML/Excel atualizado
