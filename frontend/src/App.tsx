import { useEffect, useState } from "react";
import {
  apiFetch,
  AuthError,
  authMe,
  clearAuth,
  d1Iso,
  getHistorico,
  getResultados,
  isAuthenticated,
  OcResultado,
  urlRelatorioExcel,
  urlRelatorioHtml,
  UsuarioMe,
  validar,
  ValidarResponse,
} from "./api/client";
import { AdminUsers } from "./components/AdminUsers";
import { SummaryCards } from "./components/Cards";
import { Login } from "./components/Login";
import { ResultadosTable } from "./components/ResultadosTable";
import { TrocarSenha } from "./components/TrocarSenha";

type Tela = "dashboard" | "admin";

export default function App() {
  const [user, setUser] = useState<UsuarioMe | null>(null);
  const [carregandoSessao, setCarregandoSessao] = useState(true);
  const [tela, setTela] = useState<Tela>("dashboard");
  const [trocandoSenha, setTrocandoSenha] = useState(false);

  // Restaura sessão ao montar (se houver credenciais no sessionStorage)
  useEffect(() => {
    if (!isAuthenticated()) {
      setCarregandoSessao(false);
      return;
    }
    authMe()
      .then((u) => setUser(u))
      .catch(() => {
        clearAuth();
        setUser(null);
      })
      .finally(() => setCarregandoSessao(false));
  }, []);

  function logout() {
    clearAuth();
    setUser(null);
    setTela("dashboard");
  }

  if (carregandoSessao) {
    return <div style={{ padding: 24 }}>Carregando sessão...</div>;
  }

  if (!user) {
    return <Login onLogin={(u) => setUser(u)} />;
  }

  // 1º login: força troca de senha antes de qualquer coisa
  const forcarTroca = user.must_change_password;

  return (
    <>
      {forcarTroca && (
        <TrocarSenha
          username={user.username}
          obrigatorio
          onSucesso={() =>
            setUser({ ...user, must_change_password: false })
          }
        />
      )}

      {trocandoSenha && !forcarTroca && (
        <TrocarSenha
          username={user.username}
          onSucesso={() => setTrocandoSenha(false)}
          onCancelar={() => setTrocandoSenha(false)}
        />
      )}

      <Shell
        user={user}
        tela={tela}
        onTela={setTela}
        onLogout={logout}
        onTrocarSenha={() => setTrocandoSenha(true)}
      >
        {tela === "dashboard" ? <Dashboard /> : <AdminUsers />}
      </Shell>
    </>
  );
}

// ----------------- Shell (header + nav + content) -----------------

interface ShellProps {
  user: UsuarioMe;
  tela: Tela;
  onTela: (t: Tela) => void;
  onLogout: () => void;
  onTrocarSenha: () => void;
  children: React.ReactNode;
}

function Shell({ user, tela, onTela, onLogout, onTrocarSenha, children }: ShellProps) {
  const isAdmin = (user.perfil_nome || "").toLowerCase() === "admin";
  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: 24 }}>
      <header
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
          gap: 16,
        }}
      >
        <div>
          <h1 style={{ margin: 0, fontSize: 24 }}>Validador OC — Magna Proteção</h1>
          <div style={{ color: "#5a6c7f", fontSize: 13 }}>
            Automação da validação diária de Ordens de Compra
          </div>
        </div>

        <nav style={{ display: "flex", gap: 8 }}>
          <button
            onClick={() => onTela("dashboard")}
            style={tela === "dashboard" ? navAtivo : navInativo}
          >
            Dashboard
          </button>
          {isAdmin && (
            <button
              onClick={() => onTela("admin")}
              style={tela === "admin" ? navAtivo : navInativo}
            >
              Usuários
            </button>
          )}
        </nav>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ textAlign: "right", fontSize: 13 }}>
            <div style={{ fontWeight: 600 }}>{user.nome}</div>
            <div style={{ color: "#5a6c7f", fontSize: 11 }}>
              {user.username} · {user.perfil_nome}
            </div>
          </div>
          <button onClick={onTrocarSenha} style={btnSec}>
            Trocar senha
          </button>
          <button onClick={onLogout} style={btnSec}>
            Sair
          </button>
        </div>
      </header>

      {children}
    </div>
  );
}

// ----------------- Dashboard (validação) -----------------

function Dashboard() {
  const [dataD1, setDataD1] = useState<string>(d1Iso());
  const [dryRun, setDryRun] = useState<boolean>(true);
  const [rodando, setRodando] = useState<boolean>(false);
  const [erro, setErro] = useState<string | null>(null);
  const [ultima, setUltima] = useState<ValidarResponse | null>(null);
  const [resultados, setResultados] = useState<OcResultado[]>([]);
  const [historico, setHistorico] = useState<
    {
      id: number;
      data_d1: string;
      total_ocs: number;
      aprovadas: number;
      divergentes: number;
      aguardando_ml?: number | null;
      ja_processadas?: number | null;
    }[]
  >([]);

  async function carregarHistorico() {
    try {
      const h = await getHistorico(10);
      setHistorico(h);
    } catch (e) {
      if (e instanceof AuthError) {
        // Sessão expirou — recarrega para forçar login
        window.location.reload();
        return;
      }
      console.error(e);
    }
  }

  useEffect(() => {
    carregarHistorico();
  }, []);

  async function rodar() {
    setErro(null);
    setRodando(true);
    try {
      const r = await validar(dataD1, dryRun);
      setUltima(r);
      const res = await getResultados(r.validacao_id);
      setResultados(res);
      await carregarHistorico();
    } catch (e: unknown) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      setErro(e instanceof Error ? e.message : String(e));
    } finally {
      setRodando(false);
    }
  }

  async function baixarRelatorio(data: string, tipo: "html" | "excel") {
    setErro(null);
    try {
      const url =
        tipo === "html" ? urlRelatorioHtml(data) : urlRelatorioExcel(data);
      const resp = await apiFetch(url);
      if (!resp.ok) {
        throw new Error(`Erro ${resp.status} ao gerar relatório`);
      }
      const blob = await resp.blob();
      const blobUrl = URL.createObjectURL(blob);
      if (tipo === "html") {
        window.open(blobUrl, "_blank", "noopener");
        // Revoga depois de 60s para a aba ter chance de carregar o conteúdo.
        setTimeout(() => URL.revokeObjectURL(blobUrl), 60_000);
      } else {
        const a = document.createElement("a");
        a.href = blobUrl;
        a.download = `validacao_${data}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(blobUrl);
      }
    } catch (e: unknown) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  async function abrirHistorico(validacaoId: number, data: string) {
    try {
      const res = await getResultados(validacaoId);
      setResultados(res);
      setUltima({
        validacao_id: validacaoId,
        data_d1: data,
        total: res.length,
        aprovadas: res.filter((r) => r.status === "aprovada").length,
        divergentes: res.filter((r) => r.status === "divergencia").length,
        bloqueadas: res.filter((r) => r.status === "bloqueada").length,
        aguardando_ml: res.filter((r) => r.status === "aguardando_ml").length,
        ja_processadas: res.filter((r) => r.status === "ja_processada").length,
        dry_run: true,
        relatorio_html: `${data}_validacao.html`,
        relatorio_xlsx: `${data}_validacao.xlsx`,
      });
      setDataD1(data);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      console.error(e);
    }
  }

  return (
    <>
      <div
        style={{
          background: "white",
          borderRadius: 8,
          padding: 16,
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
          boxShadow: "0 1px 3px rgba(0,0,0,.06)",
        }}
      >
        <label style={{ fontSize: 13, color: "#5a6c7f" }}>
          Data (D-1)
          <input
            type="date"
            value={dataD1}
            onChange={(e) => setDataD1(e.target.value)}
            disabled={rodando}
            style={{
              marginLeft: 8,
              padding: "6px 10px",
              border: "1px solid #d1d5db",
              borderRadius: 6,
              fontSize: 14,
            }}
          />
        </label>

        <label style={{ fontSize: 13, color: "#5a6c7f" }}>
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            disabled={rodando}
          />
          {" "}
          Dry run (não altera Pipefy)
        </label>

        <button
          onClick={rodar}
          disabled={rodando}
          style={{
            marginLeft: "auto",
            background: "#2563eb",
            color: "white",
            border: 0,
            borderRadius: 6,
            padding: "10px 20px",
            fontSize: 14,
            fontWeight: 600,
          }}
        >
          {rodando ? "Executando..." : "Puxar dados do D-1"}
        </button>

        {ultima && (
          <>
            <button
              type="button"
              onClick={() => baixarRelatorio(ultima.data_d1, "html")}
              style={btnLink}
              title="Abrir relatório completo em nova aba"
            >
              📄 Ver HTML
            </button>
            <button
              type="button"
              onClick={() => baixarRelatorio(ultima.data_d1, "excel")}
              style={btnLink}
              title="Baixar planilha com 2 abas: Cards (validação completa) + OCs sem card no Pipefy"
            >
              📥 Baixar Excel completo (cards + órfãs)
            </button>
          </>
        )}
      </div>

      {erro && (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            background: "#fecaca",
            color: "#991b1b",
            borderRadius: 6,
          }}
        >
          <strong>Erro:</strong> {erro}
        </div>
      )}

      {ultima && (
        <SummaryCards
          total={ultima.total}
          aprovadas={ultima.aprovadas}
          divergentes={ultima.divergentes}
          bloqueadas={ultima.bloqueadas ?? 0}
          aguardandoMl={ultima.aguardando_ml ?? 0}
          jaProcessadas={ultima.ja_processadas ?? 0}
        />
      )}

      <ResultadosTable resultados={resultados} />

      {historico.length > 0 && (
        <div style={{ marginTop: 32 }}>
          <h2 style={{ fontSize: 16, marginBottom: 8 }}>Últimas validações</h2>
          <div
            style={{
              background: "white",
              borderRadius: 8,
              padding: 8,
              boxShadow: "0 1px 3px rgba(0,0,0,.06)",
            }}
          >
            {historico.map((h) => (
              <div
                key={h.id}
                onClick={() => abrirHistorico(h.id, h.data_d1)}
                style={{
                  padding: "8px 12px",
                  display: "flex",
                  gap: 16,
                  fontSize: 13,
                  cursor: "pointer",
                  borderRadius: 4,
                }}
                onMouseEnter={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = "#f4f6fa")
                }
                onMouseLeave={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = "transparent")
                }
              >
                <span style={{ color: "#5a6c7f" }}>#{h.id}</span>
                <span style={{ fontWeight: 600 }}>{h.data_d1}</span>
                <span>{h.total_ocs} OCs</span>
                <span style={{ color: "#17a34a" }}>{h.aprovadas} aprov</span>
                <span style={{ color: "#ea580c" }}>{h.divergentes} div</span>
                {h.aguardando_ml != null && h.aguardando_ml > 0 && (
                  <span style={{ color: "#d97706" }}>{h.aguardando_ml} ML</span>
                )}
                {h.ja_processadas != null && h.ja_processadas > 0 && (
                  <span style={{ color: "#6b7280" }}>{h.ja_processadas} proc</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  );
}

const btnLink: React.CSSProperties = {
  background: "#f3f4f6",
  color: "#1a2332",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  padding: "8px 14px",
  fontSize: 13,
  fontWeight: 500,
  textDecoration: "none",
};

const btnSec: React.CSSProperties = {
  background: "#f3f4f6",
  color: "#1a2332",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  padding: "6px 12px",
  fontSize: 12,
  cursor: "pointer",
};

const navAtivo: React.CSSProperties = {
  background: "#1a2332",
  color: "white",
  border: 0,
  borderRadius: 6,
  padding: "8px 16px",
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
};

const navInativo: React.CSSProperties = {
  ...navAtivo,
  background: "transparent",
  color: "#1a2332",
  border: "1px solid #d1d5db",
};
