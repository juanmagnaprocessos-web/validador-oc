// Cliente HTTP simples para o backend validador-oc.
// Em dev, o proxy do Vite redireciona /api/* → http://localhost:8000/*.
// Autenticação: HTTP Basic. As credenciais são guardadas no
// sessionStorage e injetadas em todas as chamadas pelo `apiFetch`.

const BASE = import.meta.env.VITE_API_URL || "/api";
const STORAGE_KEY = "validador.auth";

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
  ocs_orfas?: number;
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
  valor_card: number | null;
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

// ----- Auth -----

export interface UsuarioMe {
  id: number;
  username: string;
  nome: string;
  email: string | null;
  perfil_id: number;
  perfil_nome: string | null;
  ativo: boolean;
  must_change_password: boolean;
  criado_em: string;
  ultimo_login: string | null;
}

export interface PerfilApi {
  id: number;
  nome: string;
  descricao: string | null;
  permissoes: string[];
  criado_em: string;
}

interface StoredCreds {
  username: string;
  basic: string; // header value: "Basic base64(user:pass)"
}

export function setAuth(username: string, password: string): void {
  const basic = "Basic " + btoa(`${username}:${password}`);
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ username, basic }));
}

export function clearAuth(): void {
  sessionStorage.removeItem(STORAGE_KEY);
}

export function getAuth(): StoredCreds | null {
  const raw = sessionStorage.getItem(STORAGE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function isAuthenticated(): boolean {
  return getAuth() !== null;
}

export class AuthError extends Error {
  constructor(message = "Não autenticado") {
    super(message);
    this.name = "AuthError";
  }
}

export async function apiFetch(input: string, init: RequestInit = {}): Promise<Response> {
  const creds = getAuth();
  const headers = new Headers(init.headers || {});
  if (creds) headers.set("Authorization", creds.basic);
  const resp = await fetch(input, { ...init, headers });
  if (resp.status === 401) {
    clearAuth();
    throw new AuthError();
  }
  return resp;
}

async function asJson<T>(resp: Response): Promise<T> {
  const text = await resp.text();
  if (!resp.ok) {
    let detail = "";
    try {
      const j = JSON.parse(text);
      detail = j.detail || JSON.stringify(j);
    } catch {
      detail = text;
    }
    throw new Error(`${resp.status}: ${detail}`);
  }
  return JSON.parse(text);
}

// ----- Endpoints de validação -----

export async function validar(
  data: string,
  dryRun: boolean,
): Promise<ValidarResponse> {
  const url = `${BASE}/validar?data=${encodeURIComponent(data)}&dry_run=${dryRun}`;
  return asJson(await apiFetch(url, { method: "POST" }));
}

export async function getHistorico(limite = 30): Promise<HistoricoEntry[]> {
  return asJson(await apiFetch(`${BASE}/historico?limite=${limite}`));
}

export async function getResultados(
  validacaoId: number,
): Promise<OcResultado[]> {
  return asJson(await apiFetch(`${BASE}/validacoes/${validacaoId}/resultados`));
}

export function urlRelatorioHtml(data: string): string {
  // Browser não envia Authorization em <a href>; mas o navegador
  // reusa o cache de credenciais Basic Auth da sessão atual.
  return `${BASE}/relatorio/${data}`;
}

export function urlRelatorioExcel(data: string): string {
  return `${BASE}/relatorio/${data}/excel`;
}

// ----- Endpoints de auth -----

export async function authMe(): Promise<UsuarioMe> {
  return asJson(await apiFetch(`${BASE}/auth/me`));
}

export async function authTrocarSenha(
  senhaAtual: string,
  novaSenha: string,
): Promise<void> {
  await asJson(
    await apiFetch(`${BASE}/auth/trocar-senha`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ senha_atual: senhaAtual, nova_senha: novaSenha }),
    }),
  );
}

export async function tentarLogin(
  username: string,
  password: string,
): Promise<UsuarioMe> {
  // Seta credenciais temporariamente e bate em /auth/me
  setAuth(username, password);
  try {
    return await authMe();
  } catch (e) {
    clearAuth();
    throw e;
  }
}

// ----- Endpoints admin -----

export async function adminListarUsuarios(): Promise<UsuarioMe[]> {
  return asJson(await apiFetch(`${BASE}/admin/usuarios`));
}

export async function adminCriarUsuario(payload: {
  username: string;
  nome: string;
  email?: string | null;
  perfil_id: number;
  senha_temporaria: string;
}): Promise<UsuarioMe> {
  return asJson(
    await apiFetch(`${BASE}/admin/usuarios`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  );
}

export async function adminAtualizarUsuario(
  id: number,
  payload: {
    nome?: string;
    email?: string | null;
    perfil_id?: number;
    ativo?: boolean;
  },
): Promise<UsuarioMe> {
  return asJson(
    await apiFetch(`${BASE}/admin/usuarios/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  );
}

export async function adminResetSenha(
  id: number,
): Promise<{ nova_senha_temporaria: string }> {
  return asJson(
    await apiFetch(`${BASE}/admin/usuarios/${id}/reset-senha`, {
      method: "POST",
    }),
  );
}

export async function adminInativarUsuario(id: number): Promise<void> {
  await asJson(
    await apiFetch(`${BASE}/admin/usuarios/${id}`, { method: "DELETE" }),
  );
}

export async function adminListarPerfis(): Promise<PerfilApi[]> {
  return asJson(await apiFetch(`${BASE}/admin/perfis`));
}

// D-1 do dia atual no formato YYYY-MM-DD
export function d1Iso(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}
