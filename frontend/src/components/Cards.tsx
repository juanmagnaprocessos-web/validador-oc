import { HistoricoEntry } from "../api/client";

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
        gridTemplateColumns: "repeat(6, 1fr)",
        gap: 12,
        margin: "24px 0",
      }}
    >
      <Card label="Total OCs" value={total} color="#1a2332" />
      <Card label="Aprovadas" value={aprovadas} color="#17a34a" />
      <Card label="Divergências" value={divergentes} color="#ea580c" />
      <Card label="Bloqueadas" value={bloqueadas} color="#dc2626" />
      <Card label="Aguardando ML" value={aguardandoMl} color="#d97706" />
      <Card label="Já processadas" value={jaProcessadas} color="#6b7280" />
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
      style={{
        background: "white",
        borderRadius: 8,
        padding: 14,
        boxShadow: "0 1px 3px rgba(0,0,0,.06)",
      }}
    >
      <div
        style={{
          fontSize: 11,
          textTransform: "uppercase",
          letterSpacing: 0.5,
          color: "#5a6c7f",
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

export function resumirHistorico(entries: HistoricoEntry[]) {
  return entries.slice(0, 1)[0];
}
