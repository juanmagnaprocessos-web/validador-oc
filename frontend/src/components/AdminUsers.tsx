import { useEffect, useState } from "react";
import {
  PerfilApi,
  UsuarioMe,
  adminAtualizarUsuario,
  adminCriarUsuario,
  adminListarPerfis,
  adminListarUsuarios,
  adminResetSenha,
} from "../api/client";
import { Spinner } from "./Spinner";
import {
  COLORS,
  RADIUS,
  baseInput,
  baseLabel,
  btnPrimary,
  btnSmall,
  cardPanel,
  errorBox,
  thStyle,
  tdStyle,
} from "../styles/theme";

export function AdminUsers() {
  const [usuarios, setUsuarios] = useState<UsuarioMe[]>([]);
  const [perfis, setPerfis] = useState<PerfilApi[]>([]);
  const [erro, setErro] = useState<string | null>(null);
  const [carregando, setCarregando] = useState(true);
  const [criando, setCriando] = useState(false);
  const [resetMsg, setResetMsg] = useState<string | null>(null);

  // form criar
  const [novoUsername, setNovoUsername] = useState("");
  const [novoNome, setNovoNome] = useState("");
  const [novoEmail, setNovoEmail] = useState("");
  const [novoPerfilId, setNovoPerfilId] = useState<number | null>(null);
  const [novoSenha, setNovoSenha] = useState("");

  async function recarregar() {
    setCarregando(true);
    setErro(null);
    try {
      const [us, ps] = await Promise.all([
        adminListarUsuarios(),
        adminListarPerfis(),
      ]);
      setUsuarios(us);
      setPerfis(ps);
      if (ps.length > 0 && novoPerfilId === null) {
        setNovoPerfilId(ps[0].id);
      }
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    } finally {
      setCarregando(false);
    }
  }

  useEffect(() => {
    recarregar();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function criarUsuario(e: React.FormEvent) {
    e.preventDefault();
    setErro(null);
    if (novoSenha.length < 8) {
      setErro("A senha temporaria precisa ter ao menos 8 caracteres");
      return;
    }
    if (novoPerfilId === null) {
      setErro("Selecione um perfil");
      return;
    }
    try {
      await adminCriarUsuario({
        username: novoUsername.trim(),
        nome: novoNome.trim(),
        email: novoEmail.trim() || null,
        perfil_id: novoPerfilId,
        senha_temporaria: novoSenha,
      });
      setNovoUsername("");
      setNovoNome("");
      setNovoEmail("");
      setNovoSenha("");
      setCriando(false);
      await recarregar();
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  async function toggleAtivo(u: UsuarioMe) {
    try {
      await adminAtualizarUsuario(u.id, { ativo: !u.ativo });
      await recarregar();
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  async function resetar(u: UsuarioMe) {
    if (!confirm(`Resetar a senha de ${u.username}?`)) return;
    try {
      const r = await adminResetSenha(u.id);
      setResetMsg(
        `Nova senha temporaria de ${u.username}: ${r.nova_senha_temporaria} (copie agora -- so sera exibida uma vez)`,
      );
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <section aria-label="Gerenciamento de usuarios">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <h2 style={{ margin: 0, fontSize: 20, color: COLORS.text }}>Usuarios e perfis</h2>
        <button
          onClick={() => setCriando(!criando)}
          style={btnPrimary}
          aria-label={criando ? "Cancelar criacao de usuario" : "Criar novo usuario"}
        >
          {criando ? "Cancelar" : "+ Novo usuario"}
        </button>
      </div>

      {erro && (
        <div style={{ ...errorBox, marginBottom: 16 }} role="alert">
          {erro}
        </div>
      )}

      {resetMsg && (
        <div
          style={{
            padding: 12,
            background: COLORS.warningBg,
            color: COLORS.warningFg,
            borderRadius: RADIUS.sm,
            marginBottom: 16,
            fontFamily: "monospace",
            fontSize: 13,
            display: "flex",
            alignItems: "center",
            gap: 12,
          }}
          role="alert"
        >
          <span style={{ flex: 1 }}>{resetMsg}</span>
          <button
            onClick={() => setResetMsg(null)}
            style={{
              ...btnSmall,
              background: "transparent",
              border: `1px solid ${COLORS.warningFg}`,
              color: COLORS.warningFg,
            }}
            aria-label="Fechar mensagem de reset de senha"
          >
            ok
          </button>
        </div>
      )}

      {criando && (
        <form
          onSubmit={criarUsuario}
          style={{
            ...cardPanel,
            padding: 20,
            marginBottom: 16,
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 14,
          }}
          aria-label="Formulario de criacao de usuario"
        >
          <div>
            <label style={baseLabel}>Username</label>
            <input
              required
              value={novoUsername}
              onChange={(e) => setNovoUsername(e.target.value)}
              style={baseInput}
              aria-label="Username do novo usuario"
            />
          </div>
          <div>
            <label style={baseLabel}>Nome completo</label>
            <input
              required
              value={novoNome}
              onChange={(e) => setNovoNome(e.target.value)}
              style={baseInput}
              aria-label="Nome completo do novo usuario"
            />
          </div>
          <div>
            <label style={baseLabel}>E-mail (opcional)</label>
            <input
              type="email"
              value={novoEmail}
              onChange={(e) => setNovoEmail(e.target.value)}
              style={baseInput}
              aria-label="Email do novo usuario"
            />
          </div>
          <div>
            <label style={baseLabel}>Perfil</label>
            <select
              required
              value={novoPerfilId ?? ""}
              onChange={(e) => setNovoPerfilId(Number(e.target.value))}
              style={baseInput}
              aria-label="Perfil do novo usuario"
            >
              {perfis.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.nome}
                </option>
              ))}
            </select>
          </div>
          <div style={{ gridColumn: "span 2" }}>
            <label style={baseLabel}>Senha temporaria (min. 8 chars)</label>
            <input
              required
              minLength={8}
              value={novoSenha}
              onChange={(e) => setNovoSenha(e.target.value)}
              style={baseInput}
              aria-label="Senha temporaria"
            />
            <div style={{ fontSize: 11, color: COLORS.textSecondary, marginTop: 4 }}>
              O usuario sera obrigado a trocar essa senha no primeiro login.
            </div>
          </div>
          <div style={{ gridColumn: "span 2", textAlign: "right" }}>
            <button type="submit" style={btnPrimary} aria-label="Criar usuario">
              Criar
            </button>
          </div>
        </form>
      )}

      {carregando ? (
        <div style={{ padding: 32, textAlign: "center" }}>
          <Spinner size={32} label="Carregando usuarios..." />
        </div>
      ) : (
        <div style={{ ...cardPanel, overflow: "hidden" }}>
          <div style={{ overflowX: "auto" }}>
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                minWidth: 700,
              }}
            >
              <thead>
                <tr style={{ background: COLORS.bgHover, color: COLORS.text }}>
                  <th style={thStyle} scope="col">ID</th>
                  <th style={thStyle} scope="col">Username</th>
                  <th style={thStyle} scope="col">Nome</th>
                  <th style={thStyle} scope="col">E-mail</th>
                  <th style={thStyle} scope="col">Perfil</th>
                  <th style={thStyle} scope="col">Ativo</th>
                  <th style={thStyle} scope="col">Ultimo login</th>
                  <th style={thStyle} scope="col">Acoes</th>
                </tr>
              </thead>
              <tbody>
                {usuarios.map((u) => (
                  <tr key={u.id} style={{ borderTop: `1px solid ${COLORS.borderRow}` }}>
                    <td style={tdStyle}>{u.id}</td>
                    <td style={{ ...tdStyle, fontFamily: "monospace" }}>{u.username}</td>
                    <td style={tdStyle}>{u.nome}</td>
                    <td style={tdStyle}>{u.email || "--"}</td>
                    <td style={tdStyle}>{u.perfil_nome || "?"}</td>
                    <td style={tdStyle}>
                      <span
                        style={{
                          padding: "2px 8px",
                          borderRadius: 4,
                          fontSize: 11,
                          fontWeight: 600,
                          background: u.ativo ? COLORS.successBg : COLORS.errorBg,
                          color: u.ativo ? COLORS.successFg : COLORS.errorFg,
                        }}
                      >
                        {u.ativo ? "Sim" : "Nao"}
                      </span>
                    </td>
                    <td style={{ ...tdStyle, fontSize: 11, color: COLORS.textSecondary }}>
                      {u.ultimo_login || "--"}
                    </td>
                    <td style={tdStyle}>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button
                          onClick={() => resetar(u)}
                          style={btnSmall}
                          aria-label={`Resetar senha de ${u.username}`}
                        >
                          Reset senha
                        </button>
                        <button
                          onClick={() => toggleAtivo(u)}
                          style={btnSmall}
                          aria-label={u.ativo ? `Inativar usuario ${u.username}` : `Reativar usuario ${u.username}`}
                        >
                          {u.ativo ? "Inativar" : "Reativar"}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}
