import { COLORS } from "../styles/theme";

interface Props {
  size?: number;
  color?: string;
  label?: string;
}

export function Spinner({ size = 32, color = COLORS.primary, label }: Props) {
  const strokeWidth = Math.max(2, size / 10);
  const r = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * r;

  return (
    <div
      role="status"
      aria-live="polite"
      aria-label={label || "Carregando"}
      style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 8 }}
    >
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        style={{ animation: "validador-spin 0.9s linear infinite" }}
      >
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke={COLORS.borderLight}
          strokeWidth={strokeWidth}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke={color}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={`${circumference * 0.7} ${circumference * 0.3}`}
        />
      </svg>
      {label && (
        <span style={{ fontSize: 13, color: COLORS.textSecondary }}>{label}</span>
      )}
      <style>{`
        @keyframes validador-spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}
