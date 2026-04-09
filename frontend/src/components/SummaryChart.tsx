import { COLORS } from "../styles/theme";

interface Segment {
  label: string;
  value: number;
  color: string;
}

interface Props {
  aprovadas: number;
  divergentes: number;
  bloqueadas: number;
  aguardandoMl: number;
  jaProcessadas: number;
}

export function SummaryChart({ aprovadas, divergentes, bloqueadas, aguardandoMl, jaProcessadas }: Props) {
  const segments: Segment[] = [
    { label: "Aprovadas", value: aprovadas, color: COLORS.success },
    { label: "Divergencias", value: divergentes, color: COLORS.warning },
    { label: "Bloqueadas", value: bloqueadas, color: COLORS.danger },
    { label: "Aguardando ML", value: aguardandoMl, color: COLORS.warningAmber },
    { label: "Ja processadas", value: jaProcessadas, color: COLORS.textMuted },
  ].filter((s) => s.value > 0);

  const total = segments.reduce((a, s) => a + s.value, 0);
  if (total === 0) return null;

  const size = 160;
  const strokeWidth = 28;
  const center = size / 2;
  const r = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * r;

  let accumulated = 0;

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 24,
        flexWrap: "wrap",
        justifyContent: "center",
      }}
      aria-label="Grafico resumo de validacoes"
      role="img"
    >
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle
          cx={center}
          cy={center}
          r={r}
          fill="none"
          stroke={COLORS.borderLight}
          strokeWidth={strokeWidth}
        />
        {segments.map((seg) => {
          const pct = seg.value / total;
          const dashLen = circumference * pct;
          const dashGap = circumference - dashLen;
          const offset = circumference * 0.25 - circumference * accumulated;
          accumulated += pct;

          return (
            <circle
              key={seg.label}
              cx={center}
              cy={center}
              r={r}
              fill="none"
              stroke={seg.color}
              strokeWidth={strokeWidth}
              strokeDasharray={`${dashLen} ${dashGap}`}
              strokeDashoffset={offset}
              strokeLinecap="butt"
              style={{ transition: "stroke-dasharray 400ms ease" }}
            />
          );
        })}
        <text
          x={center}
          y={center - 6}
          textAnchor="middle"
          dominantBaseline="central"
          style={{ fontSize: 22, fontWeight: 700, fill: COLORS.text }}
        >
          {total}
        </text>
        <text
          x={center}
          y={center + 14}
          textAnchor="middle"
          dominantBaseline="central"
          style={{ fontSize: 10, fill: COLORS.textSecondary, textTransform: "uppercase", letterSpacing: 0.5 }}
        >
          Total OCs
        </text>
      </svg>

      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {segments.map((seg) => (
          <div key={seg.label} style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span
              style={{
                width: 12,
                height: 12,
                borderRadius: 3,
                background: seg.color,
                flexShrink: 0,
              }}
            />
            <span style={{ fontSize: 13, color: COLORS.text }}>{seg.label}</span>
            <span style={{ fontSize: 13, fontWeight: 600, color: COLORS.text, marginLeft: "auto", paddingLeft: 12 }}>
              {seg.value}
            </span>
            <span style={{ fontSize: 11, color: COLORS.textSecondary, minWidth: 36, textAlign: "right" }}>
              {Math.round((seg.value / total) * 100)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
