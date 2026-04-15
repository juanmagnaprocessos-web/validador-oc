import { useState } from "react";
import type React from "react";
import { apiFetch, urlRelatorioExcel, urlRelatorioHtml, type HistoricoEntry } from "../api/client";
import { COLORS, btnSmall } from "../styles/theme";

interface Props {
  item: HistoricoEntry;
  onSelecionar: (id: number, dataD1: string) => void;
}

async function baixarRelatorio(data: string, tipo: "html" | "excel"): Promise<void> {
  const url = tipo === "html" ? urlRelatorioHtml(data) : urlRelatorioExcel(data);
  const resp = await apiFetch(url);
  if (!resp.ok) throw new Error(`Erro ${resp.status} ao gerar relatório`);
  const blob = await resp.blob();
  const blobUrl = URL.createObjectURL(blob);
  if (tipo === "html") {
    window.open(blobUrl, "_blank", "noopener");
    setTimeout(() => URL.revokeObjectURL(blobUrl), 60_000);
  } else {
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = `validacao_${data}.xlsx`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(blobUrl);
  }
}

const badgeBase: React.CSSProperties = {
  fontSize: 10,
  padding: "2px 8px",
  borderRadius: 999,
  fontWeight: 600,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

export function HistoricoRow({ item, onSelecionar }: Props) {
  const ehCron = item.origem === "cron";
  const [baixando, setBaixando] = useState<"html" | "excel" | null>(null);

  const handleDownload = async (
    e: React.MouseEvent,
    tipo: "html" | "excel",
  ) => {
    e.stopPropagation();
    if (baixando) return;
    setBaixando(tipo);
    try {
      await baixarRelatorio(item.data_d1, tipo);
    } catch (err) {
      console.error(err);
    } finally {
      setBaixando(null);
    }
  };

  return (
    <div
      role="button"
      tabIndex={0}
      aria-label={`Abrir validação ${item.data_d1} com ${item.total_ocs} OCs`}
      onClick={() => onSelecionar(item.id, item.data_d1)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelecionar(item.id, item.data_d1);
        }
      }}
      style={{
        padding: "12px 14px",
        display: "grid",
        gridTemplateColumns: "minmax(110px, 0.8fr) auto auto auto auto auto 1fr auto",
        gap: 14,
        fontSize: 13,
        alignItems: "center",
        borderBottom: `1px solid ${COLORS.borderRow}`,
        cursor: "pointer",
        transition: "background 100ms ease",
      }}
      onMouseEnter={(e) =>
        ((e.currentTarget as HTMLDivElement).style.background = COLORS.bg)
      }
      onMouseLeave={(e) =>
        ((e.currentTarget as HTMLDivElement).style.background = "transparent")
      }
    >
      <span style={{ fontWeight: 600, color: COLORS.text }}>{item.data_d1}</span>

      <span title="Total de OCs" style={{ color: COLORS.textSecondary }}>
        {item.total_ocs} OCs
      </span>

      <span title="Aprovadas" style={{ color: COLORS.success }}>
        {item.aprovadas} aprovadas
      </span>

      <span title="Divergências" style={{ color: COLORS.warning }}>
        {item.divergentes} divergências
      </span>

      {item.aguardando_ml != null && item.aguardando_ml > 0 ? (
        <span title="Aguardando Mercado Livre" style={{ color: COLORS.warningAmber }}>
          {item.aguardando_ml} ML
        </span>
      ) : (
        <span />
      )}

      {item.ja_processadas != null && item.ja_processadas > 0 ? (
        <span title="Já processadas" style={{ color: COLORS.textMuted }}>
          {item.ja_processadas} processadas
        </span>
      ) : (
        <span />
      )}

      <span
        style={{
          ...badgeBase,
          background: ehCron ? `${COLORS.primary}20` : `${COLORS.textMuted}20`,
          color: ehCron ? COLORS.primary : COLORS.textSecondary,
          justifySelf: "start",
        }}
        title={ehCron ? "Execução automática pelo CRON das 02:00" : "Execução manual pelo analista"}
      >
        {ehCron ? "Automático" : "Manual"}
      </span>

      <div style={{ display: "flex", gap: 6, justifySelf: "end" }}>
        <button
          type="button"
          onClick={(e) => handleDownload(e, "html")}
          disabled={baixando !== null}
          style={{
            ...btnSmall,
            padding: "4px 10px",
            opacity: baixando === "html" ? 0.6 : 1,
          }}
          title="Baixar relatório HTML"
          aria-busy={baixando === "html"}
        >
          {baixando === "html" ? "..." : "HTML"}
        </button>
        <button
          type="button"
          onClick={(e) => handleDownload(e, "excel")}
          disabled={baixando !== null}
          style={{
            ...btnSmall,
            padding: "4px 10px",
            opacity: baixando === "excel" ? 0.6 : 1,
          }}
          title="Baixar relatório Excel"
          aria-busy={baixando === "excel"}
        >
          {baixando === "excel" ? "..." : "Excel"}
        </button>
      </div>
    </div>
  );
}
