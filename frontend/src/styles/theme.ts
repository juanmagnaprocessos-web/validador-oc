import type React from "react";

export const COLORS = {
  primary: "#2563eb",
  primaryHover: "#1d4ed8",
  danger: "#dc2626",
  success: "#17a34a",
  warning: "#ea580c",
  warningAmber: "#d97706",
  text: "#1a2332",
  textSecondary: "#5a6c7f",
  textMuted: "#6b7280",
  bg: "#f4f6fa",
  bgWhite: "#ffffff",
  bgHover: "#f9fafb",
  border: "#d1d5db",
  borderLight: "#e5e7eb",
  borderRow: "#eef1f5",
  errorBg: "#fecaca",
  errorFg: "#991b1b",
  successBg: "#d1fae5",
  successFg: "#065f46",
  warningBg: "#fef3c7",
  warningFg: "#92400e",
} as const;

export const SHADOWS = {
  sm: "0 1px 3px rgba(0,0,0,.06)",
  md: "0 4px 12px rgba(0,0,0,.08)",
  lg: "0 8px 24px rgba(0,0,0,.15)",
} as const;

export const RADIUS = { sm: 6, md: 10, lg: 16 } as const;

export const baseInput: React.CSSProperties = {
  width: "100%",
  padding: "10px 12px",
  border: `1px solid ${COLORS.border}`,
  borderRadius: RADIUS.sm,
  fontSize: 14,
  boxSizing: "border-box",
  fontFamily: "inherit",
  color: COLORS.text,
  background: COLORS.bgWhite,
  outline: "none",
};

export const baseButton: React.CSSProperties = {
  border: 0,
  borderRadius: RADIUS.sm,
  fontSize: 14,
  fontWeight: 600,
  cursor: "pointer",
  fontFamily: "inherit",
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
  transition: "background 150ms ease, opacity 150ms ease",
};

export const baseLabel: React.CSSProperties = {
  display: "block",
  fontSize: 12,
  color: COLORS.textSecondary,
  marginBottom: 4,
  textTransform: "uppercase",
  letterSpacing: 0.5,
  fontWeight: 500,
};

export const btnPrimary: React.CSSProperties = {
  ...baseButton,
  background: COLORS.primary,
  color: "#ffffff",
  padding: "10px 20px",
};

export const btnSecondary: React.CSSProperties = {
  ...baseButton,
  background: "#f3f4f6",
  color: COLORS.text,
  border: `1px solid ${COLORS.border}`,
  padding: "8px 14px",
  fontSize: 13,
  fontWeight: 500,
};

export const btnSmall: React.CSSProperties = {
  ...baseButton,
  background: "#f3f4f6",
  border: `1px solid ${COLORS.border}`,
  borderRadius: 4,
  padding: "4px 10px",
  fontSize: 12,
  fontWeight: 500,
  color: COLORS.text,
};

export const btnNav = (active: boolean): React.CSSProperties => ({
  ...baseButton,
  background: active ? COLORS.text : "transparent",
  color: active ? "#ffffff" : COLORS.text,
  border: active ? 0 : `1px solid ${COLORS.border}`,
  borderRadius: RADIUS.sm,
  padding: "8px 16px",
  fontSize: 13,
});

export const cardPanel: React.CSSProperties = {
  background: COLORS.bgWhite,
  borderRadius: RADIUS.md,
  boxShadow: SHADOWS.sm,
};

export const errorBox: React.CSSProperties = {
  padding: 12,
  background: COLORS.errorBg,
  color: COLORS.errorFg,
  borderRadius: RADIUS.sm,
  fontSize: 13,
};

export const thStyle: React.CSSProperties = {
  padding: "14px 14px",
  textAlign: "left",
  fontSize: 11,
  textTransform: "uppercase",
  letterSpacing: 0.5,
  fontWeight: 600,
};

export const tdStyle: React.CSSProperties = {
  padding: "12px 14px",
  fontSize: 13,
  verticalAlign: "middle",
};
