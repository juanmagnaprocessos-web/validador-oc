// Cliente HTTP simples para o backend validador-oc.
// Em dev, o proxy do Vite redireciona /api/* → http://localhost:8000/*.

const BASE = "/api";

export type StatusValidacao =
  | "aprovada"
  | "divergencia"
  | "bloqueada"
  | "aguardando_ml"
  | "ja_processada";

export interface ValidarResponse {
  validacao_id: number;
  data_d1: string;
  total: number;
  aprovadas: number;
  divergentes: number;
  bloqueadas: number;
  aguardando_ml: number;
  ja_processadas: number;
  dry_run: boolean;
  relatorio_html: string;
  relatorio_xlsx: string;
}

export interface OcResultado {
  id: number;
  validacao_id: number;
  id_pedido: string;
  id_cotacao: string | null;
  placa: string | null;
  placa_normalizada: string | null;
  fornecedor: string | null;
  comprador: string | null;
  forma_pagamento: string | null;
  valor_club: number | null;
  valor_pdf: number | null;
  valor_cilia: number | null;
  qtd_cotacoes: number | null;
  qtd_produtos: number | null;
  peca_duplicada: string;
  status: StatusValidacao;
  regras_falhadas: Array<{ regra: string; titulo: string }> | null;
  fase_pipefy: string | null;
  fase_pipefy_atual: string | null;
  card_pipefy_id: string | null;
}

export interface HistoricoEntry {
  id: number;
  data_execucao: string;
  data_d1: string;
  total_ocs: number;
  aprovadas: number;
  divergentes: number;
  bloqueadas: number;
  aguardando_ml: number | null;
  ja_processadas: number | null;
  status: string;
  dry_run: number;
  executado_por: string | null;
}

export async function validar(
  data: string,
  dryRun: boolean
): Promise<ValidarResponse> {
  const url = `${BASE}/validar?data=${encodeURIComponent(data)}&dry_run=${dryRun}`;
  const resp = await fetch(url, { method: "POST" });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`${resp.status}: ${txt}`);
  }
  return resp.json();
}

export async function getHistorico(limite = 30): Promise<HistoricoEntry[]> {
  const resp = await fetch(`${BASE}/historico?limite=${limite}`);
  if (!resp.ok) throw new Error(`${resp.status}`);
  return resp.json();
}

export async function getResultados(
  validacaoId: number
): Promise<OcResultado[]> {
  const resp = await fetch(
    `${BASE}/validacoes/${validacaoId}/resultados`
  );
  if (!resp.ok) throw new Error(`${resp.status}`);
  return resp.json();
}

export function urlRelatorioHtml(data: string): string {
  return `${BASE}/relatorio/${data}`;
}

export function urlRelatorioExcel(data: string): string {
  return `${BASE}/relatorio/${data}/excel`;
}

// D-1 do dia atual no formato YYYY-MM-DD
export function d1Iso(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}
