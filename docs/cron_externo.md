# CRON externo via cron-job.org

Garante que o job diário rode mesmo se o Render Free hibernar antes das 02h BRT (APScheduler in-process não dispara quando a app está dormindo).

## Como funciona

1. Backend expõe `POST /api/cron/trigger`, autenticado por header `X-Cron-Token` comparado em tempo constante contra a ENV `CRON_TRIGGER_TOKEN`.
2. Endpoint dispara `run_daily_validation_job(None)` em background (D-1 calculado em BRT) — não aceita `data_d1` via query, blindando contra backfill abusivo se o token vazar.
3. Lock atômico em `cron_locks` impede dupla execução se o cron-job.org retentar.
4. cron-job.org bate no endpoint às 02h05 BRT (5 min após o agendamento interno do APScheduler — assim, se o APScheduler já rodou, o cron externo só vê `ja_em_execucao` e segue em frente).

## Setup — Backend (Render)

1. Gerar token aleatório forte (em qualquer terminal):
   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```
   Exemplo: `xK9_aB3cD7eF2gH5jL8mN1pQ4rS7tV0wY3zB6cE9fG2`

2. No painel do Render → Service → Environment:
   - Adicionar `CRON_TRIGGER_TOKEN=<o token gerado>` (sync: false).
3. Salvar e aguardar o redeploy.
4. Verificar:
   ```bash
   # Sem token → 403
   curl -i -X POST https://validador-oc.onrender.com/api/cron/trigger
   # Token correto → 200 {"status":"disparado"}
   curl -i -X POST https://validador-oc.onrender.com/api/cron/trigger \
     -H "X-Cron-Token: xK9_aB3cD7eF2gH5jL8mN1pQ4rS7tV0wY3zB6cE9fG2"
   ```

## Setup — cron-job.org

1. Criar conta gratuita em <https://cron-job.org> com seu email Magna.
2. **Cronjobs** → **Create cronjob**.
3. Preencher:
   - **Title**: `Validador OC — disparo diário`
   - **URL**: `https://validador-oc.onrender.com/api/cron/trigger`
   - **Schedule**:
     - Selecionar **Every weekday** (Mon–Fri) ou **Every day** conforme política
     - **Time**: `02:05` (BRT)
     - **Timezone**: `America/Sao_Paulo`
4. **Advanced**:
   - **Request method**: `POST`
   - **Request body**: deixar vazio
   - **Headers** → adicionar:
     - Name: `X-Cron-Token`
     - Value: `<o token gerado no passo 1>`
   - **Notifications**: ativar notificação por email para falhas (>= 3 falhas seguidas).
5. **Save**.

## Validação

- No dashboard do cron-job.org, o job aparece com último status `200 OK`.
- No próximo D-1, o `/api/cron/status` mostra registro em `ultimo_lock` e o banner "CRON não executou" desaparece.

## Em caso de problema

- **403 forbidden**: header `X-Cron-Token` não bate com a ENV. Recriar a ENV no Render e validar via curl.
- **503 trigger externo nao configurado**: ENV `CRON_TRIGGER_TOKEN` ausente ou vazia.
- **`ja_em_execucao`**: APScheduler in-process já está rodando (esperado quando app não dormiu). Não é erro.
- **Render acordando**: a primeira chamada do dia pode demorar 30-60s (cold start). cron-job.org tem timeout default de 30s — aumentar para 60s na config do job se houver falha.

## Pegadinhas

- O endpoint NÃO aceita `data_d1` via query — só processa o D-1 atual em BRT. Para backfill manual, usar o endpoint admin `POST /api/admin/cron/run-now?data_d1=YYYY-MM-DD` (Basic Auth admin).
- Se o token vazar em log do cron-job.org ou no histórico de execução, **rotacionar imediatamente**: gerar novo token, atualizar ENV no Render, atualizar header no cron-job.org.
- O endpoint loga apenas `disparado` ou `falhou` — nunca o token nem placas.

## Pré-requisito de segurança

Antes de habilitar o cron externo (que torna o sistema acessível por chamada não-autenticada por sessão de usuário), trocar as senhas default `admin/admin123` e `juanpablo/admin123` no banco de produção e configurar `must_change_password=1` para ambos. Sem isso, o sistema fica vulnerável a brute-force que já não tem gating de rate-limit. Esta proteção é independente do trigger externo, mas é pré-condição para considerar o ambiente "production-ready".
