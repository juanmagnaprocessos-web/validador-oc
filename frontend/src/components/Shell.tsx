import type { ReactNode } from "react";
import { useAuth } from "../contexts/AuthContext";
import { useTheme } from "../contexts/ThemeContext";
import { COLORS, RADIUS, btnNav, btnSecondary, FONT } from "../styles/theme";

interface ShellProps {
  tela: "dashboard" | "admin";
  onTela: (t: "dashboard" | "admin") => void;
  onTrocarSenha: () => void;
  children: ReactNode;
}

export function Shell({ tela, onTela, onTrocarSenha, children }: ShellProps) {
  const { user, logout } = useAuth();
  const { theme, toggle } = useTheme();
  if (!user) return null;

  const isAdmin = (user.perfil_nome || "").toLowerCase() === "admin";

  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      {/* Top bar — hairline sticky */}
      <div
        style={{
          position: "sticky",
          top: 0,
          zIndex: 20,
          background: "var(--header-bg)",
          backdropFilter: "blur(16px)",
          WebkitBackdropFilter: "blur(16px)",
          borderBottom: `1px solid ${COLORS.border}`,
        }}
      >
        {/* Fio de accent no topo */}
        <div
          style={{
            height: 1,
            background: `linear-gradient(90deg, transparent, ${COLORS.primary} 30%, ${COLORS.primary} 70%, transparent)`,
            opacity: 0.55,
          }}
        />

        <header
          style={{
            maxWidth: 1400,
            margin: "0 auto",
            padding: "14px 24px",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 16,
            flexWrap: "wrap",
          }}
        >
          {/* Brand */}
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <MagnaBadge />
            <div
              style={{
                width: 1,
                height: 28,
                background: COLORS.border,
              }}
              aria-hidden
            />
            <div>
              <div
                style={{
                  fontSize: 14,
                  fontWeight: 600,
                  color: COLORS.text,
                  letterSpacing: -0.2,
                  lineHeight: 1.2,
                }}
              >
                Validador OC
              </div>
              <div
                style={{
                  fontSize: 10,
                  color: COLORS.textMuted,
                  textTransform: "uppercase",
                  letterSpacing: 1.2,
                  fontWeight: 500,
                  marginTop: 2,
                }}
              >
                Processos Magna
              </div>
            </div>
          </div>

          {/* Nav segmented */}
          <nav
            style={{
              display: "flex",
              gap: 2,
              padding: 3,
              background: COLORS.bgWhite,
              border: `1px solid ${COLORS.border}`,
              borderRadius: RADIUS.md,
            }}
            aria-label="Navegação principal"
          >
            <button
              onClick={() => onTela("dashboard")}
              style={btnNav(tela === "dashboard")}
              aria-current={tela === "dashboard" ? "page" : undefined}
            >
              Dashboard
            </button>
            {isAdmin && (
              <button
                onClick={() => onTela("admin")}
                style={btnNav(tela === "admin")}
                aria-current={tela === "admin" ? "page" : undefined}
              >
                Usuários
              </button>
            )}
          </nav>

          {/* User menu */}
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ textAlign: "right", fontSize: 12 }}>
              <div style={{ fontWeight: 600, color: COLORS.text, letterSpacing: -0.1 }}>
                {user.nome}
              </div>
              <div
                style={{
                  color: COLORS.textMuted,
                  fontSize: 10,
                  fontFamily: FONT.mono,
                  marginTop: 1,
                }}
              >
                {user.username} · {user.perfil_nome}
              </div>
            </div>
            <div
              style={{
                width: 1,
                height: 26,
                background: COLORS.border,
              }}
              aria-hidden
            />
            <button
              onClick={toggle}
              style={{
                ...btnSecondary,
                padding: "5px 8px",
                fontSize: 14,
                minWidth: 32,
                lineHeight: 1,
              }}
              aria-label={
                theme === "dark" ? "Alternar para tema claro" : "Alternar para tema escuro"
              }
              title={theme === "dark" ? "Tema claro" : "Tema escuro"}
            >
              {theme === "dark" ? <SunIcon /> : <MoonIcon />}
            </button>
            <button
              onClick={onTrocarSenha}
              style={{ ...btnSecondary, padding: "5px 10px", fontSize: 11 }}
              aria-label="Trocar senha do usuário"
              title="Trocar senha"
            >
              Senha
            </button>
            <button
              onClick={logout}
              style={{ ...btnSecondary, padding: "5px 10px", fontSize: 11 }}
              aria-label="Sair do sistema"
              title="Sair"
            >
              Sair
            </button>
          </div>
        </header>
      </div>

      <main
        style={{
          maxWidth: 1400,
          margin: "0 auto",
          padding: "24px",
          width: "100%",
          flex: 1,
          animation: "fade-in-up 420ms cubic-bezier(0.2, 0.8, 0.2, 1)",
        }}
      >
        {children}
      </main>

      {/* Rodapé minimal */}
      <footer
        style={{
          maxWidth: 1400,
          margin: "0 auto",
          padding: "16px 24px 24px",
          width: "100%",
          fontSize: 10,
          color: COLORS.textMuted,
          fontFamily: FONT.mono,
          letterSpacing: 0.5,
          textTransform: "uppercase",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          borderTop: `1px solid ${COLORS.border}`,
          marginTop: 24,
        }}
      >
        <span>Validador OC v0.1 · Operações Magna</span>
        <span>
          <span style={{ color: COLORS.success }}>●</span> Online
        </span>
      </footer>
    </div>
  );
}

/** Ícones sol/lua para toggle de tema */
function SunIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  );
}

/** Badge com o logo oficial da Magna */
function MagnaBadge() {
  return (
    <div
      style={{
        height: 38,
        width: 38,
        borderRadius: 8,
        background: "#ffffff",
        border: `1px solid ${COLORS.border}`,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 3,
        flexShrink: 0,
        boxShadow: "0 1px 3px rgba(0,0,0,0.3)",
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
  );
}
