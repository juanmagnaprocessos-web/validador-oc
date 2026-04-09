import { useState, useMemo, useEffect } from "react";
import { OcResultado } from "../api/client";
import { COLORS, cardPanel, thStyle, tdStyle, btnSecondary } from "../styles/theme";

interface Props {
  resultados: OcResultado[];
}

const STATUS_LABEL: Record<
  string,
  { text: string; bg: string; fg: string }
> = {
  aprovada: { text: "Aprovada", bg: COLORS.successBg, fg: COLORS.successFg },
  divergencia: { text: "Divergencia", bg: "#fed7aa", fg: "#9a3412" },
  bloqueada: { text: "Bloqueada", bg: COLORS.errorBg, fg: COLORS.errorFg },
  aguardando_ml: { text: "ML -- Manual", bg: COLORS.warningBg, fg: COLORS.warningFg },
  ja_processada: { text: "Ja processada", bg: COLORS.borderLight, fg: "#374151" },
};

type SortCol = "id_pedido" | "placa" | "fornecedor" | "comprador" | "valor_club" | "valor_pdf" | "qtd_cotacoes" | "status";
type SortDir = "asc" | "desc";

const PAGE_SIZE = 20;

function fmtMoney(v: number | null) {
  if (v == null) return "--";
  return v.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

export function ResultadosTable({ resultados }: Props) {
  const [sortCol, setSortCol] = useState<SortCol | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [page, setPage] = useState(0);

  // Resetar paginacao quando resultados mudam
  useEffect(() => setPage(0), [resultados]);

  const sorted = useMemo(() => {
    if (!sortCol) return resultados;
    const arr = [...resultados];
    arr.sort((a, b) => {
      let va: string | number | null = null;
      let vb: string | number | null = null;

      switch (sortCol) {
        case "id_pedido": va = a.id_pedido; vb = b.id_pedido; break;
        case "placa": va = a.placa; vb = b.placa; break;
        case "fornecedor": va = a.fornecedor; vb = b.fornecedor; break;
        case "comprador": va = a.comprador; vb = b.comprador; break;
        case "valor_club": va = a.valor_club; vb = b.valor_club; break;
        case "valor_pdf": va = a.valor_pdf; vb = b.valor_pdf; break;
        case "qtd_cotacoes": va = a.qtd_cotacoes; vb = b.qtd_cotacoes; break;
        case "status": va = a.status; vb = b.status; break;
      }

      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;

      let cmp = 0;
      if (typeof va === "number" && typeof vb === "number") {
        cmp = va - vb;
      } else {
        cmp = String(va).localeCompare(String(vb), "pt-BR");
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return arr;
  }, [resultados, sortCol, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const paged = sorted.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  function toggleSort(col: SortCol) {
    if (sortCol === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(col);
      setSortDir("asc");
    }
    setPage(0);
  }

  function sortIndicator(col: SortCol) {
    if (sortCol !== col) return " \u2195";
    return sortDir === "asc" ? " \u2191" : " \u2193";
  }

  if (resultados.length === 0) {
    return (
      <div
        style={{
          ...cardPanel,
          padding: 48,
          textAlign: "center",
        }}
      >
        <svg width="48" height="48" viewBox="0 0 48 48" style={{ opacity: 0.3, marginBottom: 12 }}>
          <rect x="4" y="8" width="40" height="32" rx="4" fill="none" stroke={COLORS.textSecondary} strokeWidth="2" />
          <line x1="12" y1="18" x2="36" y2="18" stroke={COLORS.textSecondary} strokeWidth="2" />
          <line x1="12" y1="24" x2="30" y2="24" stroke={COLORS.textSecondary} strokeWidth="2" />
          <line x1="12" y1="30" x2="26" y2="30" stroke={COLORS.textSecondary} strokeWidth="2" />
        </svg>
        <div style={{ color: COLORS.textSecondary, fontSize: 14 }}>
          Nenhum resultado ainda.
        </div>
        <div style={{ color: COLORS.textMuted, fontSize: 13, marginTop: 4 }}>
          Clique em "Puxar dados do D-1" para rodar a validacao ou selecione uma validacao do historico.
        </div>
      </div>
    );
  }

  const columns: { label: string; col: SortCol | null; align?: "right" }[] = [
    { label: "N. OC", col: "id_pedido" },
    { label: "Placa", col: "placa" },
    { label: "Fornecedor", col: "fornecedor" },
    { label: "Comprador", col: "comprador" },
    { label: "Valor Club", col: "valor_club", align: "right" },
    { label: "Valor PDF", col: "valor_pdf", align: "right" },
    { label: "Cot.", col: "qtd_cotacoes", align: "right" },
    { label: "Status", col: "status" },
    { label: "Motivo", col: null },
    { label: "Fase Atual", col: null },
    { label: "Fase Destino", col: null },
  ];

  return (
    <div>
      <div
        style={{
          ...cardPanel,
          overflow: "hidden",
        }}
      >
        <div style={{ overflowX: "auto", WebkitOverflowScrolling: "touch" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 900 }}>
            <thead>
              <tr style={{ background: COLORS.text, color: "#ffffff" }}>
                {columns.map((c) => (
                  <th
                    key={c.label}
                    scope="col"
                    style={{
                      ...thStyle,
                      textAlign: c.align || "left",
                      cursor: c.col ? "pointer" : "default",
                      userSelect: "none",
                      whiteSpace: "nowrap",
                    }}
                    onClick={c.col ? () => toggleSort(c.col!) : undefined}
                    aria-sort={
                      c.col && sortCol === c.col
                        ? sortDir === "asc" ? "ascending" : "descending"
                        : undefined
                    }
                    tabIndex={c.col ? 0 : undefined}
                    onKeyDown={c.col ? (e) => { if (e.key === "Enter") toggleSort(c.col!); } : undefined}
                  >
                    {c.label}
                    {c.col && sortIndicator(c.col)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {paged.map((r) => {
                const st = STATUS_LABEL[r.status] ?? STATUS_LABEL.aprovada;
                let motivo = (r.regras_falhadas ?? [])
                  .map((d) => `[${d.regra}] ${d.titulo}`)
                  .join("; ");
                if (r.status === "aguardando_ml") {
                  motivo = "Fornecedor Mercado Livre -- validacao manual";
                } else if (r.status === "ja_processada") {
                  motivo = `Card ja estava em "${r.fase_pipefy_atual ?? "?"}"`;
                }
                return (
                  <tr key={r.id} style={{ borderTop: `1px solid ${COLORS.borderRow}` }}>
                    <td style={tdStyle}>{r.id_pedido}</td>
                    <td style={tdStyle}>{r.placa ?? "--"}</td>
                    <td style={{ ...tdStyle, maxWidth: 180 }}>{(r.fornecedor ?? "").slice(0, 30)}</td>
                    <td style={tdStyle}>{r.comprador ?? "--"}</td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>{fmtMoney(r.valor_club)}</td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>{fmtMoney(r.valor_pdf)}</td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>{r.qtd_cotacoes ?? 0}</td>
                    <td style={tdStyle}>
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
                    <td style={{ ...tdStyle, color: "#b91c1c", fontSize: 12, maxWidth: 320 }}>
                      {motivo || "--"}
                    </td>
                    <td style={tdStyle}>{r.fase_pipefy_atual ?? "--"}</td>
                    <td style={tdStyle}>{r.fase_pipefy ?? "--"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Paginacao */}
      {sorted.length > PAGE_SIZE && (
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: 12,
            padding: "0 4px",
          }}
          aria-label="Paginacao da tabela"
        >
          <span style={{ fontSize: 13, color: COLORS.textSecondary }}>
            {safePage * PAGE_SIZE + 1}--{Math.min((safePage + 1) * PAGE_SIZE, sorted.length)} de {sorted.length}
          </span>
          <div style={{ display: "flex", gap: 6 }}>
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={safePage === 0}
              style={{ ...btnSecondary, padding: "6px 14px", fontSize: 12 }}
              aria-label="Pagina anterior"
            >
              Anterior
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={safePage >= totalPages - 1}
              style={{ ...btnSecondary, padding: "6px 14px", fontSize: 12 }}
              aria-label="Proxima pagina"
            >
              Proximo
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
