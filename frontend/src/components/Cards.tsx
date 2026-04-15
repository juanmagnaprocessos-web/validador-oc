import { COLORS, RADIUS, FONT } from "../styles/theme";

interface Props {
  total: number;
  aprovadas: number;
  divergentes: number;
  bloqueadas: number;
  aguardandoMl: number;
  jaProcessadas: number;
}

export function SummaryCards({
  total,
  aprovadas,
  divergentes,
  bloqueadas,
  aguardandoMl,
  jaProcessadas,
}: Props) {
  const items: Array<{ label: string; value: number; tone: CardTone; delta?: number }> = [
    { label: "Total OCs", value: total, tone: "neutral" },
    { label: "Aprovadas", value: aprovadas, tone: "success", delta: pct(aprovadas, total) },
    { label: "Divergências", value: divergentes, tone: "warning", delta: pct(divergentes, total) },
    { label: "Bloqueadas", value: bloqueadas, tone: "danger", delta: pct(bloqueadas, total) },
    { label: "Aguardando ML", value: aguardandoMl, tone: "amber", delta: pct(aguardandoMl, total) },
    { label: "Já processadas", value: jaProcessadas, tone: "muted", delta: pct(jaProcessadas, total) },
  ];
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
        gap: 10,
      }}
      role="list"
      aria-label="Resumo dos resultados"
    >
      {items.map((it, i) => (
        <Card
          key={it.label}
          label={it.label}
          value={it.value}
          tone={it.tone}
          delta={it.delta}
          style={{ animationDelay: `${i * 40}ms` }}
        />
      ))}
    </div>
  );
}

type CardTone = "neutral" | "success" | "warning" | "danger" | "amber" | "muted";

function pct(value: number, total: number): number {
  if (!total) return 0;
  return Math.round((value / total) * 100);
}

function toneColor(t: CardTone): string {
  switch (t) {
    case "success":
      return COLORS.success;
    case "warning":
      return COLORS.warning;
    case "danger":
      return COLORS.danger;
    case "amber":
      return COLORS.warningAmber;
    case "muted":
      return COLORS.textMuted;
    default:
      return COLORS.text;
  }
}

function Card({
  label,
  value,
  tone,
  delta,
  style,
}: {
  label: string;
  value: number;
  tone: CardTone;
  delta?: number;
  style?: React.CSSProperties;
}) {
  const color = toneColor(tone);
  return (
    <div
      role="listitem"
      style={{
        position: "relative",
        background: COLORS.bgWhite,
        borderRadius: RADIUS.md,
        padding: "14px 16px",
        border: `1px solid ${COLORS.border}`,
        overflow: "hidden",
        animation: "fade-in-up 420ms cubic-bezier(0.2, 0.8, 0.2, 1) both",
        transition: "border-color 120ms ease, transform 120ms ease",
        ...style,
      }}
      aria-label={`${label}: ${value}`}
    >
      {/* Indicador de tom (hairline vertical à esquerda, bem sutil) */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          left: 0,
          top: 10,
          bottom: 10,
          width: 2,
          background: color,
          opacity: tone === "neutral" ? 0 : 0.55,
          borderRadius: "0 2px 2px 0",
        }}
      />

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <span
          style={{
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: 1,
            color: COLORS.textMuted,
            fontWeight: 500,
          }}
        >
          {label}
        </span>
        {typeof delta === "number" && delta > 0 && (
          <span
            style={{
              fontSize: 10,
              color: COLORS.textMuted,
              fontFamily: FONT.mono,
              fontVariantNumeric: "tabular-nums",
            }}
          >
            {delta}%
          </span>
        )}
      </div>
      <div
        style={{
          fontSize: 28,
          fontWeight: 600,
          color,
          marginTop: 8,
          fontFamily: FONT.mono,
          fontVariantNumeric: "tabular-nums",
          letterSpacing: -0.02,
          lineHeight: 1,
        }}
      >
        {value}
      </div>
    </div>
  );
}
