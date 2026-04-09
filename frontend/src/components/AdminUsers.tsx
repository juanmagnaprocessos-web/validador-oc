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
      setErro("A senha temporária precisa ter ao menos 8 caracteres");
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
        `Nova senha temporária de ${u.username}: ${r.nova_senha_temporaria} (copie agora — só será exibida uma vez)`,
      );
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <h2 style={{ margin: 0, fontSize: 20 }}>Usuários e perfis</h2>
        <button onClick={() => setCriando(!criando)} style={btnPri}>
          {criando ? "Cancelar" : "+ Novo usuário"}
        </button>
      </div>

      {erro && (
        <div
          style={{
            padding: 12,
            background: "#fecaca",
            color: "#991b1b",
            borderRadius: 6,
            marginBottom: 16,
          }}
        >
          {erro}
        </div>
      )}

      {resetMsg && (
        <div
          style={{
            padding: 12,
            background: "#fef3c7",
            color: "#92400e",
            borderRadius: 6,
            marginBottom: 16,
            fontFamily: "monospace",
            fontSize: 13,
          }}
        >
          {resetMsg}
          <button
            onClick={() => setResetMsg(null)}
            style={{
              marginLeft: 12,
              background: "transparent",
              border: "1px solid #92400e",
              color: "#92400e",
              padding: "2px 8px",
              borderRadius: 4,
              cursor: "pointer",
            }}
          >
            ok
          </button>
        </div>
      )}

      {criando && (
        <form
          onSubmit={criarUsuario}
          style={{
            background: "white",
            padding: 16,
            borderRadius: 8,
            marginBottom: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,.06)",
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 12,
          }}
        >
          <div>
            <label style={lbl}>Username</label>
            <input
              required
              value={novoUsername}
              onChange={(e) => setNovoUsername(e.target.value)}
              style={inp}
            />
          </div>
          <div>
            <label style={lbl}>Nome completo</label>
            <input
              required
              value={novoNome}
              onChange={(e) => setNovoNome(e.target.value)}
              style={inp}
            />
          </div>
          <div>
            <label style={lbl}>E-mail (opcional)</label>
            <input
              type="email"
              value={novoEmail}
              onChange={(e) => setNovoEmail(e.target.value)}
              style={inp}
            />
          </div>
          <div>
            <label style={lbl}>Perfil</label>
            <select
              required
              value={novoPerfilId ?? ""}
              onChange={(e) => setNovoPerfilId(Number(e.target.value))}
              style={inp}
            >
              {perfis.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.nome}
                </option>
              ))}
            </select>
          </div>
          <div style={{ gridColumn: "span 2" }}>
            <label style={lbl}>Senha temporária (mín. 8 chars)</label>
            <input
              required
              minLength={8}
              value={novoSenha}
              onChange={(e) => setNovoSenha(e.target.value)}
              style={inp}
            />
            <div style={{ fontSize: 11, color: "#5a6c7f", marginTop: 4 }}>
              O usuário será obrigado a trocar essa senha no primeiro login.
            </div>
          </div>
          <div style={{ gridColumn: "span 2", textAlign: "right" }}>
            <button type="submit" style={btnPri}>
              Criar
            </button>
          </div>
        </form>
      )}

      {carregando ? (
        <div>Carregando...</div>
      ) : (
        <table
          style={{
            width: "100%",
            background: "white",
            borderCollapse: "collapse",
            borderRadius: 8,
            overflow: "hidden",
            boxShadow: "0 1px 3px rgba(0,0,0,.06)",
          }}
        >
          <thead>
            <tr style={{ background: "#1a2332", color: "white" }}>
              <th style={th}>ID</th>
              <th style={th}>Username</th>
              <th style={th}>Nome</th>
              <th style={th}>E-mail</th>
              <th style={th}>Perfil</th>
              <th style={th}>Ativo</th>
              <th style={th}>Último login</th>
              <th style={th}>Ações</th>
            </tr>
          </thead>
          <tbody>
            {usuarios.map((u) => (
              <tr key={u.id} style={{ borderTop: "1px solid #eef1f5" }}>
                <td style={td}>{u.id}</td>
                <td style={{ ...td, fontFamily: "monospace" }}>{u.username}</td>
                <td style={td}>{u.nome}</td>
                <td style={td}>{u.email || "—"}</td>
                <td style={td}>{u.perfil_nome || "?"}</td>
                <td style={td}>
                  <span
                    style={{
                      padding: "2px 8px",
                      borderRadius: 4,
                      fontSize: 11,
                      fontWeight: 600,
                      background: u.ativo ? "#d1fae5" : "#fecaca",
                      color: u.ativo ? "#065f46" : "#991b1b",
                    }}
                  >
                    {u.ativo ? "Sim" : "Não"}
                  </span>
                </td>
                <td style={{ ...td, fontSize: 11, color: "#5a6c7f" }}>
                  {u.ultimo_login || "—"}
                </td>
                <td style={td}>
                  <button onClick={() => resetar(u)} style={btnSm}>
                    Reset senha
                  </button>{" "}
                  <button onClick={() => toggleAtivo(u)} style={btnSm}>
                    {u.ativo ? "Inativar" : "Reativar"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

const lbl: React.CSSProperties = {
  display: "block",
  fontSize: 12,
  color: "#5a6c7f",
  marginBottom: 4,
};

const inp: React.CSSProperties = {
  width: "100%",
  padding: "8px 10px",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  fontSize: 14,
  boxSizing: "border-box",
};

const th: React.CSSProperties = {
  padding: "10px 12px",
  textAlign: "left",
  fontSize: 11,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const td: React.CSSProperties = {
  padding: "10px 12px",
  fontSize: 13,
  verticalAlign: "middle",
};

const btnPri: React.CSSProperties = {
  background: "#2563eb",
  color: "white",
  border: 0,
  borderRadius: 6,
  padding: "8px 16px",
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
};

const btnSm: React.CSSProperties = {
  background: "#f3f4f6",
  border: "1px solid #d1d5db",
  borderRadius: 4,
  padding: "4px 10px",
  fontSize: 12,
  cursor: "pointer",
};
