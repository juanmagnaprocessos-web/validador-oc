import type React from "react";

/**
 * Tokens do tema — referenciam CSS variables declaradas em index.css.
 * Isso permite alternar entre tema escuro e claro em tempo real via
 * `document.documentElement.setAttribute("data-theme", "light"|"dark")`
 * sem precisar recriar componentes.
 */

export const COLORS = {
  // Brand Magna
  brandRed: "var(--brand-red)",
  brandNavy: "var(--brand-navy)",
  navyAccent: "var(--navy-accent)",
  navyDim: "var(--navy-dim)",

  primary: "var(--accent)",
  primaryHover: "var(--accent-hover)",
  primaryDim: "var(--accent-dim)",

  danger: "var(--danger)",
  dangerDim: "var(--danger-bg)",
  success: "var(--success)",
  successDim: "var(--success-bg)",
  warning: "var(--warning)",
  warningAmber: "var(--warning-amber)",

  text: "var(--text-primary)",
  textSecondary: "var(--text-secondary)",
  textMuted: "var(--text-muted)",

  bg: "var(--bg-base)",
  bgWhite: "var(--surface)",
  bgHover: "var(--surface-hover)",

  border: "var(--border)",
  borderLight: "var(--border-row)",
  borderRow: "var(--border-row)",

  errorBg: "var(--danger-bg)",
  errorFg: "var(--danger-fg)",
  successBg: "var(--success-bg)",
  successFg: "var(--success-fg)",
  warningBg: "var(--warning-bg)",
  warningFg: "var(--warning-fg)",
} as const;

export const SHADOWS = {
  sm: "var(--shadow-sm)",
  md: "var(--shadow-md)",
  lg: "var(--shadow-lg)",
} as const;

export const RADIUS = { sm: 4, md: 6, lg: 10 } as const;

export const FONT = {
  sans: 'var(--font-sans)',
  mono: 'var(--font-mono)',
} as const;

export const baseInput: React.CSSProperties = {
  width: "100%",
  padding: "10px 12px",
  border: `1px solid ${COLORS.border}`,
  borderRadius: RADIUS.sm,
  fontSize: 13,
  boxSizing: "border-box",
  fontFamily: "inherit",
  color: COLORS.text,
  background: COLORS.bg,
  outline: "none",
  transition: "border-color 120ms ease, box-shadow 120ms ease",
};

export const baseButton: React.CSSProperties = {
  border: 0,
  borderRadius: RADIUS.sm,
  fontSize: 13,
  fontWeight: 500,
  cursor: "pointer",
  fontFamily: "inherit",
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
  transition: "background 120ms ease, border-color 120ms ease, color 120ms ease, transform 80ms ease",
  letterSpacing: -0.01,
};

export const baseLabel: React.CSSProperties = {
  display: "block",
  fontSize: 10,
  color: COLORS.textSecondary,
  marginBottom: 6,
  textTransform: "uppercase",
  letterSpacing: 0.8,
  fontWeight: 500,
};

export const btnPrimary: React.CSSProperties = {
  ...baseButton,
  background: COLORS.primary,
  color: "#ffffff",
  padding: "10px 18px",
  fontWeight: 600,
  boxShadow: "inset 0 1px 0 rgba(255,255,255,0.08)",
};

export const btnSecondary: React.CSSProperties = {
  ...baseButton,
  background: COLORS.bgWhite,
  color: COLORS.text,
  border: `1px solid ${COLORS.border}`,
  padding: "8px 14px",
  fontSize: 12,
};

export const btnSmall: React.CSSProperties = {
  ...baseButton,
  background: "transparent",
  border: `1px solid ${COLORS.border}`,
  borderRadius: RADIUS.sm,
  padding: "4px 10px",
  fontSize: 11,
  fontWeight: 500,
  color: COLORS.textSecondary,
};

export const btnNav = (active: boolean): React.CSSProperties => ({
  ...baseButton,
  background: active ? COLORS.primaryDim : "transparent",
  color: active ? COLORS.primary : COLORS.textSecondary,
  border: `1px solid ${active ? COLORS.primaryDim : "transparent"}`,
  borderRadius: RADIUS.sm,
  padding: "6px 14px",
  fontSize: 12,
  fontWeight: active ? 600 : 500,
  position: "relative",
});

export const cardPanel: React.CSSProperties = {
  background: COLORS.bgWhite,
  borderRadius: RADIUS.md,
  border: `1px solid ${COLORS.border}`,
  boxShadow: SHADOWS.sm,
};

export const errorBox: React.CSSProperties = {
  padding: "10px 12px",
  background: COLORS.errorBg,
  color: COLORS.errorFg,
  border: `1px solid ${COLORS.danger}40`,
  borderRadius: RADIUS.sm,
  fontSize: 12,
};

export const thStyle: React.CSSProperties = {
  padding: "12px 14px",
  textAlign: "left",
  fontSize: 10,
  textTransform: "uppercase",
  letterSpacing: 0.8,
  fontWeight: 500,
  color: COLORS.textSecondary,
  borderBottom: `1px solid ${COLORS.border}`,
};

export const tdStyle: React.CSSProperties = {
  padding: "11px 14px",
  fontSize: 13,
  verticalAlign: "middle",
};
