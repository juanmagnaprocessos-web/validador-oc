import { OcResultado } from "../api/client";

interface Props {
  resultados: OcResultado[];
}

const STATUS_LABEL: Record<
  string,
  { text: string; bg: string; fg: string }
> = {
  aprovada: { text: "Aprovada", bg: "#d1fae5", fg: "#065f46" },
  divergencia: { text: "Divergência", bg: "#fed7aa", fg: "#9a3412" },
  bloqueada: { text: "Bloqueada", bg: "#fecaca", fg: "#991b1b" },
  aguardando_ml: { text: "ML — Manual", bg: "#fef3c7", fg: "#92400e" },
  ja_processada: { text: "Já processada", bg: "#e5e7eb", fg: "#374151" },
};

function fmtMoney(v: number | null) {
  if (v == null) return "—";
  return v.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

export function ResultadosTable({ resultados }: Props) {
  if (resultados.length === 0) {
    return (
      <div
        style={{
          padding: 32,
          background: "white",
          borderRadius: 8,
          textAlign: "center",
          color: "#5a6c7f",
        }}
      >
        Nenhum resultado ainda. Clique em "Puxar dados" para rodar a validação.
      </div>
    );
  }

  return (
    <div
      style={{
        background: "white",
        borderRadius: 8,
        overflow: "hidden",
        boxShadow: "0 1px 3px rgba(0,0,0,.06)",
      }}
    >
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ background: "#1a2332", color: "white" }}>
            {[
              "Nº OC",
              "Placa",
              "Fornecedor",
              "Comprador",
              "Valor Club",
              "Valor PDF",
              "Cot.",
              "Status",
              "Motivo",
              "Fase Atual",
              "Fase Destino",
            ].map((h) => (
              <th
                key={h}
                style={{
                  textAlign: h.includes("Valor") || h === "Cot." ? "right" : "left",
                  padding: "12px 10px",
                  fontSize: 11,
                  textTransform: "uppercase",
                  letterSpacing: 0.5,
                  fontWeight: 600,
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {resultados.map((r) => {
            const st = STATUS_LABEL[r.status] ?? STATUS_LABEL.aprovada;
            let motivo = (r.regras_falhadas ?? [])
              .map((d) => `[${d.regra}] ${d.titulo}`)
              .join("; ");
            if (r.status === "aguardando_ml") {
              motivo = "Fornecedor Mercado Livre — validação manual";
            } else if (r.status === "ja_processada") {
              motivo = `Card já estava em "${r.fase_pipefy_atual ?? "?"}"`;
            }
            return (
              <tr key={r.id} style={{ borderTop: "1px solid #eef1f5" }}>
                <td style={td}>{r.id_pedido}</td>
                <td style={td}>{r.placa ?? "—"}</td>
                <td style={td}>{(r.fornecedor ?? "").slice(0, 30)}</td>
                <td style={td}>{r.comprador ?? "—"}</td>
                <td style={{ ...td, textAlign: "right" }}>{fmtMoney(r.valor_club)}</td>
                <td style={{ ...td, textAlign: "right" }}>{fmtMoney(r.valor_pdf)}</td>
                <td style={{ ...td, textAlign: "right" }}>{r.qtd_cotacoes ?? 0}</td>
                <td style={td}>
                  <span
                    style={{
                      background: st.bg,
                      color: st.fg,
                      padding: "3px 8px",
                      borderRadius: 4,
                      fontSize: 11,
                      fontWeight: 600,
                      whiteSpace: "nowrap",
                    }}
                  >
                    {st.text}
                  </span>
                </td>
                <td style={{ ...td, color: "#b91c1c", fontSize: 12, maxWidth: 320 }}>
                  {motivo || "—"}
                </td>
                <td style={td}>{r.fase_pipefy_atual ?? "—"}</td>
                <td style={td}>{r.fase_pipefy ?? "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const td: React.CSSProperties = {
  padding: "10px",
  fontSize: 13,
  verticalAlign: "top",
};
