import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";
import { Spinner } from "./Spinner";
import {
  COLORS,
  SHADOWS,
  RADIUS,
  baseInput,
  baseLabel,
  btnPrimary,
  errorBox,
} from "../styles/theme";

export function Login() {
  const { login } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [erro, setErro] = useState<string | null>(null);
  const [enviando, setEnviando] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErro(null);
    setEnviando(true);
    try {
      await login(username.trim(), password);
    } catch (err: unknown) {
      setErro(
        err instanceof Error
          ? err.message.includes("401")
            ? "Usuario ou senha incorretos"
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
        background: COLORS.bg,
      }}
    >
      <form
        onSubmit={submit}
        style={{
          background: COLORS.bgWhite,
          padding: 36,
          borderRadius: RADIUS.lg,
          boxShadow: SHADOWS.md,
          width: 380,
        }}
        aria-label="Formulario de login"
      >
        <h1 style={{ margin: 0, fontSize: 24, color: COLORS.text, letterSpacing: -0.3 }}>
          Validador OC
        </h1>
        <p style={{ color: COLORS.textSecondary, fontSize: 13, marginTop: 4, marginBottom: 28 }}>
          Magna Protecao -- Acesso restrito
        </p>

        <label style={{ ...baseLabel, marginTop: 12 }}>Usuario</label>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoFocus
          required
          autoComplete="username"
          style={baseInput}
          aria-label="Nome de usuario"
        />

        <label style={{ ...baseLabel, marginTop: 16 }}>Senha</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          autoComplete="current-password"
          style={baseInput}
          aria-label="Senha"
        />

        {erro && (
          <div style={{ ...errorBox, marginTop: 14 }} role="alert">
            {erro}
          </div>
        )}

        <button
          type="submit"
          disabled={enviando}
          style={{ ...btnPrimary, marginTop: 24, width: "100%", padding: "12px 20px" }}
          aria-label={enviando ? "Entrando no sistema" : "Entrar"}
        >
          {enviando ? (
            <>
              <Spinner size={16} color="#ffffff" />
              Entrando...
            </>
          ) : (
            "Entrar"
          )}
        </button>
      </form>
    </div>
  );
}
