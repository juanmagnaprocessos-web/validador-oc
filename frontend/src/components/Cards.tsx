import { COLORS, SHADOWS, RADIUS } from "../styles/theme";

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
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
        gap: 12,
      }}
      role="list"
      aria-label="Resumo dos resultados"
    >
      <Card label="Total OCs" value={total} color={COLORS.text} />
      <Card label="Aprovadas" value={aprovadas} color={COLORS.success} />
      <Card label="Divergencias" value={divergentes} color={COLORS.warning} />
      <Card label="Bloqueadas" value={bloqueadas} color={COLORS.danger} />
      <Card label="Aguardando ML" value={aguardandoMl} color={COLORS.warningAmber} />
      <Card label="Ja processadas" value={jaProcessadas} color={COLORS.textMuted} />
    </div>
  );
}

function Card({
  label,
  value,
  color,
}: {
  label: string;
  value: number;
  color: string;
}) {
  return (
    <div
      role="listitem"
      style={{
        background: COLORS.bgWhite,
        borderRadius: RADIUS.md,
        padding: 16,
        boxShadow: SHADOWS.sm,
        borderLeft: `3px solid ${color}`,
        transition: "box-shadow 150ms ease",
      }}
      aria-label={`${label}: ${value}`}
    >
      <div
        style={{
          fontSize: 11,
          textTransform: "uppercase",
          letterSpacing: 0.5,
          color: COLORS.textSecondary,
          fontWeight: 500,
        }}
      >
        {label}
      </div>
      <div style={{ fontSize: 28, fontWeight: 700, color, marginTop: 4 }}>
        {value}
      </div>
    </div>
  );
}
