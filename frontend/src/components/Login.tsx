import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";
import { Spinner } from "./Spinner";
import {
  COLORS,
  RADIUS,
  baseInput,
  baseLabel,
  btnPrimary,
  errorBox,
  FONT,
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
        position: "relative",
        overflow: "hidden",
        padding: 24,
      }}
    >
      {/* Grid decorativo sutil no fundo */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          inset: 0,
          backgroundImage: `
            linear-gradient(${COLORS.border} 1px, transparent 1px),
            linear-gradient(90deg, ${COLORS.border} 1px, transparent 1px)
          `,
          backgroundSize: "48px 48px",
          maskImage:
            "radial-gradient(ellipse 60% 50% at 50% 50%, black 30%, transparent 70%)",
          WebkitMaskImage:
            "radial-gradient(ellipse 60% 50% at 50% 50%, black 30%, transparent 70%)",
          opacity: 0.3,
        }}
      />

      {/* Halo de accent */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          top: "15%",
          left: "50%",
          transform: "translateX(-50%)",
          width: 420,
          height: 420,
          background: `radial-gradient(circle, ${COLORS.primary}, transparent 60%)`,
          opacity: 0.08,
          pointerEvents: "none",
          filter: "blur(20px)",
        }}
      />

      <form
        onSubmit={submit}
        style={{
          position: "relative",
          background: COLORS.bgWhite,
          padding: 36,
          borderRadius: RADIUS.lg,
          border: `1px solid ${COLORS.border}`,
          boxShadow:
            "0 24px 48px rgba(0,0,0,0.5), 0 0 0 1px " + COLORS.border,
          width: 400,
          animation: "fade-in-up 600ms cubic-bezier(0.2, 0.8, 0.2, 1)",
        }}
        aria-label="Formulário de login"
      >
        {/* Slash de accent no topo */}
        <div
          aria-hidden
          style={{
            position: "absolute",
            top: -1,
            left: 24,
            right: 24,
            height: 2,
            background: `linear-gradient(90deg, transparent, ${COLORS.primary}, transparent)`,
          }}
        />

        {/* Brand row — logo oficial Magna */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 14,
            marginBottom: 28,
          }}
        >
          <div
            style={{
              width: 96,
              height: 96,
              borderRadius: 12,
              background: "#ffffff",
              border: `1px solid ${COLORS.border}`,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 8,
              boxShadow: "0 6px 24px rgba(0,0,0,0.35)",
            }}
            aria-label="Magna Proteção Automotiva"
          >
            <img
              src="/logo-magna.png"
              alt=""
              style={{
                width: "100%",
                height: "100%",
                objectFit: "contain",
                display: "block",
              }}
            />
          </div>
          <div style={{ textAlign: "center" }}>
            <div
              style={{
                fontSize: 10,
                color: COLORS.textMuted,
                textTransform: "uppercase",
                letterSpacing: 1.8,
                fontWeight: 500,
              }}
            >
              Processos · Magna
            </div>
            <h1
              style={{
                margin: 0,
                marginTop: 4,
                fontSize: 24,
                color: COLORS.text,
                letterSpacing: -0.5,
                fontWeight: 600,
                lineHeight: 1.1,
              }}
            >
              Validador OC
            </h1>
          </div>
        </div>

        <p
          style={{
            color: COLORS.textSecondary,
            fontSize: 12,
            margin: "0 0 24px",
            lineHeight: 1.5,
          }}
        >
          Acesso restrito à equipe de processos.
          <br />
          Entre com suas credenciais.
        </p>

        <label style={baseLabel}>Usuário</label>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoFocus
          required
          autoComplete="username"
          style={{ ...baseInput, fontFamily: FONT.mono }}
          aria-label="Nome de usuário"
        />

        <div style={{ height: 16 }} />

        <label style={baseLabel}>Senha</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          autoComplete="current-password"
          style={{ ...baseInput, fontFamily: FONT.mono }}
          aria-label="Senha"
        />

        {erro && (
          <div style={{ ...errorBox, marginTop: 18 }} role="alert">
            {erro}
          </div>
        )}

        <button
          type="submit"
          disabled={enviando}
          style={{
            ...btnPrimary,
            marginTop: 24,
            width: "100%",
            padding: "13px 20px",
            opacity: enviando ? 0.7 : 1,
          }}
          aria-label={enviando ? "Entrando no sistema" : "Entrar"}
        >
          {enviando ? (
            <>
              <Spinner size={14} color="#ffffff" />
              Entrando...
            </>
          ) : (
            "Entrar →"
          )}
        </button>

        <div
          style={{
            marginTop: 24,
            paddingTop: 16,
            borderTop: `1px solid ${COLORS.border}`,
            fontSize: 10,
            color: COLORS.textMuted,
            textAlign: "center",
            letterSpacing: 0.5,
            textTransform: "uppercase",
            fontFamily: FONT.mono,
          }}
        >
          Validador OC v0.1
        </div>
      </form>
    </div>
  );
}
