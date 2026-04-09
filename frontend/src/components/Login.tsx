import { useState } from "react";
import { tentarLogin, UsuarioMe } from "../api/client";

interface Props {
  onLogin: (user: UsuarioMe) => void;
}

export function Login({ onLogin }: Props) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [erro, setErro] = useState<string | null>(null);
  const [enviando, setEnviando] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErro(null);
    setEnviando(true);
    try {
      const u = await tentarLogin(username.trim(), password);
      onLogin(u);
    } catch (err: unknown) {
      setErro(
        err instanceof Error
          ? err.message.includes("401")
            ? "Usuário ou senha incorretos"
            : err.message
          : "Falha no login",
      );
    } finally {
      setEnviando(false);
    }
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "#f4f6fa",
      }}
    >
      <form
        onSubmit={submit}
        style={{
          background: "white",
          padding: 32,
          borderRadius: 12,
          boxShadow: "0 4px 12px rgba(0,0,0,.08)",
          width: 360,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 22, color: "#1a2332" }}>
          Validador OC
        </h1>
        <p style={{ color: "#5a6c7f", fontSize: 13, marginTop: 4, marginBottom: 24 }}>
          Magna Proteção · Acesso restrito
        </p>

        <label style={lbl}>Usuário</label>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoFocus
          required
          autoComplete="username"
          style={inp}
        />

        <label style={lbl}>Senha</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          autoComplete="current-password"
          style={inp}
        />

        {erro && (
          <div
            style={{
              marginTop: 12,
              padding: 10,
              background: "#fecaca",
              color: "#991b1b",
              borderRadius: 6,
              fontSize: 13,
            }}
          >
            {erro}
          </div>
        )}

        <button type="submit" disabled={enviando} style={btn}>
          {enviando ? "Entrando..." : "Entrar"}
        </button>
      </form>
    </div>
  );
}

const lbl: React.CSSProperties = {
  display: "block",
  fontSize: 12,
  color: "#5a6c7f",
  marginTop: 12,
  marginBottom: 4,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const inp: React.CSSProperties = {
  width: "100%",
  padding: "10px 12px",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  fontSize: 14,
  boxSizing: "border-box",
};

const btn: React.CSSProperties = {
  marginTop: 20,
  width: "100%",
  background: "#2563eb",
  color: "white",
  border: 0,
  borderRadius: 6,
  padding: "12px 20px",
  fontSize: 14,
  fontWeight: 600,
  cursor: "pointer",
};
