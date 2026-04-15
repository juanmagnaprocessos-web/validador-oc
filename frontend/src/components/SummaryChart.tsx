import { COLORS, FONT } from "../styles/theme";

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

export function SummaryChart({
  aprovadas,
  divergentes,
  bloqueadas,
  aguardandoMl,
  jaProcessadas,
}: Props) {
  const segments: Segment[] = [
    { label: "Aprovadas", value: aprovadas, color: COLORS.success },
    { label: "Divergências", value: divergentes, color: COLORS.warning },
    { label: "Bloqueadas", value: bloqueadas, color: COLORS.danger },
    { label: "Aguardando ML", value: aguardandoMl, color: COLORS.warningAmber },
    { label: "Já processadas", value: jaProcessadas, color: COLORS.textMuted },
  ].filter((s) => s.value > 0);

  const total = segments.reduce((a, s) => a + s.value, 0);
  if (total === 0) return null;

  const size = 168;
  const strokeWidth = 16;
  const center = size / 2;
  const r = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * r;
  const gap = 2; // gap visual entre segmentos

  let accumulated = 0;

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 32,
        flexWrap: "wrap",
        justifyContent: "flex-start",
      }}
      aria-label="Gráfico resumo de validações"
      role="img"
    >
      <div style={{ position: "relative" }}>
        <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
          <defs>
            <filter id="donutGlow" x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="1.5" result="glow" />
              <feMerge>
                <feMergeNode in="glow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>
          {/* Trilho de fundo */}
          <circle
            cx={center}
            cy={center}
            r={r}
            fill="none"
            stroke={COLORS.borderLight}
            strokeWidth={strokeWidth}
            opacity={0.5}
          />
          {segments.map((seg) => {
            const pct = seg.value / total;
            const dashLen = Math.max(circumference * pct - gap, 0.001);
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
                style={{
                  transition: "stroke-dasharray 400ms ease",
                  filter: "url(#donutGlow)",
                }}
              />
            );
          })}
          <text
            x={center}
            y={center - 6}
            textAnchor="middle"
            dominantBaseline="central"
            style={{
              fontSize: 26,
              fontWeight: 600,
              fill: COLORS.text,
              fontFamily: FONT.mono,
              letterSpacing: -0.5,
            }}
          >
            {total}
          </text>
          <text
            x={center}
            y={center + 16}
            textAnchor="middle"
            dominantBaseline="central"
            style={{
              fontSize: 9,
              fill: COLORS.textMuted,
              textTransform: "uppercase",
              letterSpacing: 1.5,
              fontWeight: 500,
            }}
          >
            Total OCs
          </text>
        </svg>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 200 }}>
        {segments.map((seg) => {
          const pct = Math.round((seg.value / total) * 100);
          return (
            <div
              key={seg.label}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "6px 8px",
                borderRadius: 4,
                transition: "background 120ms ease",
              }}
            >
              <span
                style={{
                  width: 3,
                  height: 16,
                  borderRadius: 2,
                  background: seg.color,
                  flexShrink: 0,
                  boxShadow: `0 0 8px ${seg.color}40`,
                }}
                aria-hidden
              />
              <span
                style={{
                  fontSize: 12,
                  color: COLORS.text,
                  letterSpacing: -0.1,
                  flex: 1,
                }}
              >
                {seg.label}
              </span>
              <span
                style={{
                  fontSize: 13,
                  fontWeight: 600,
                  color: COLORS.text,
                  fontFamily: FONT.mono,
                  fontVariantNumeric: "tabular-nums",
                }}
              >
                {seg.value}
              </span>
              <span
                style={{
                  fontSize: 10,
                  color: COLORS.textMuted,
                  fontFamily: FONT.mono,
                  fontVariantNumeric: "tabular-nums",
                  minWidth: 32,
                  textAlign: "right",
                }}
              >
                {pct}%
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
