import type { ReactNode } from "react";
import { useAuth } from "../contexts/AuthContext";
import { COLORS, SHADOWS, RADIUS, btnNav, btnSecondary } from "../styles/theme";

interface ShellProps {
  tela: "dashboard" | "admin";
  onTela: (t: "dashboard" | "admin") => void;
  onTrocarSenha: () => void;
  children: ReactNode;
}

export function Shell({ tela, onTela, onTrocarSenha, children }: ShellProps) {
  const { user, logout } = useAuth();
  if (!user) return null;

  const isAdmin = (user.perfil_nome || "").toLowerCase() === "admin";

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: "16px 24px 24px" }}>
      <header
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 20,
          gap: 16,
          flexWrap: "wrap",
          background: COLORS.bgWhite,
          padding: "16px 20px",
          borderRadius: RADIUS.lg,
          boxShadow: SHADOWS.sm,
        }}
      >
        <div>
          <h1 style={{ margin: 0, fontSize: 22, color: COLORS.text, letterSpacing: -0.3 }}>
            Validador OC
          </h1>
          <div style={{ color: COLORS.textSecondary, fontSize: 12, marginTop: 2 }}>
            Magna Protecao -- Automacao de Ordens de Compra
          </div>
        </div>

        <nav style={{ display: "flex", gap: 6 }} aria-label="Navegacao principal">
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
              Usuarios
            </button>
          )}
        </nav>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ textAlign: "right", fontSize: 13 }}>
            <div style={{ fontWeight: 600, color: COLORS.text }}>{user.nome}</div>
            <div style={{ color: COLORS.textSecondary, fontSize: 11 }}>
              {user.username} -- {user.perfil_nome}
            </div>
          </div>
          <button
            onClick={onTrocarSenha}
            style={{ ...btnSecondary, padding: "6px 12px", fontSize: 12 }}
            aria-label="Trocar senha do usuario"
          >
            Trocar senha
          </button>
          <button
            onClick={logout}
            style={{ ...btnSecondary, padding: "6px 12px", fontSize: 12 }}
            aria-label="Sair do sistema"
          >
            Sair
          </button>
        </div>
      </header>

      <main>{children}</main>
    </div>
  );
}
