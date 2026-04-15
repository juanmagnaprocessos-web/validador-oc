import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getHistorico, type HistoricoEntry } from "../api/client";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { COLORS, RADIUS, SHADOWS, baseInput, baseLabel, btnSecondary, errorBox } from "../styles/theme";
import { Spinner } from "./Spinner";
import { HistoricoRow } from "./HistoricoRow";

interface Props {
  open: boolean;
  onClose: () => void;
  onSelecionar: (id: number, dataD1: string) => void;
  triggerRef?: React.RefObject<HTMLElement>;
}

interface Filtros {
  limite: number;
  dataInicio: string;
  dataFim: string;
}

const LIMITE_MAX = 500;

export function HistoricoModal({ open, onClose, onSelecionar, triggerRef }: Props) {
  const [filtros, setFiltros] = useState<Filtros>({
    limite: 30,
    dataInicio: "",
    dataFim: "",
  });
  const filtrosDebounced = useDebouncedValue(filtros, 400);

  const [rows, setRows] = useState<HistoricoEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [erro, setErro] = useState<string | null>(null);
  const painelRef = useRef<HTMLDivElement>(null);
  const firstInputRef = useRef<HTMLInputElement>(null);

  const erroRange = useMemo(() => {
    if (filtros.dataInicio && filtros.dataFim && filtros.dataInicio > filtros.dataFim) {
      return "A data inicial não pode ser posterior à data final.";
    }
    return null;
  }, [filtros.dataInicio, filtros.dataFim]);

  const avisoAmplo = useMemo(() => {
    if (!filtros.dataInicio || !filtros.dataFim) return null;
    const ini = new Date(filtros.dataInicio).getTime();
    const fim = new Date(filtros.dataFim).getTime();
    const dias = Math.round((fim - ini) / 86_400_000);
    if (dias > 90) return `Intervalo de ${dias} dias pode retornar muitas linhas.`;
    return null;
  }, [filtros.dataInicio, filtros.dataFim]);

  const fetchHistorico = useCallback(async (signal: AbortSignal) => {
    if (erroRange) return;
    setLoading(true);
    setErro(null);
    try {
      const limite = Math.max(1, Math.min(LIMITE_MAX, filtrosDebounced.limite || 30));
      const data = await getHistorico(
        {
          limite,
          data_inicio: filtrosDebounced.dataInicio || undefined,
          data_fim: filtrosDebounced.dataFim || undefined,
        },
        { signal },
      );
      if (!signal.aborted) setRows(data);
    } catch (e) {
      if (!signal.aborted) {
        const msg = e instanceof Error ? e.message : String(e);
        if (!/aborted/i.test(msg)) setErro(msg);
      }
    } finally {
      if (!signal.aborted) setLoading(false);
    }
  }, [filtrosDebounced, erroRange]);

  // Fetch com AbortController em cada mudança
  useEffect(() => {
    if (!open) return;
    const ctrl = new AbortController();
    fetchHistorico(ctrl.signal);
    return () => ctrl.abort();
  }, [open, fetchHistorico]);

  // Lock scroll do body + focus inicial + escape
  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        onClose();
      } else if (e.key === "Tab" && painelRef.current) {
        // focus trap
        const focaveis = painelRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, [tabindex]:not([tabindex="-1"])',
        );
        if (focaveis.length === 0) return;
        const primeiro = focaveis[0];
        const ultimo = focaveis[focaveis.length - 1];
        if (e.shiftKey && document.activeElement === primeiro) {
          e.preventDefault();
          ultimo.focus();
        } else if (!e.shiftKey && document.activeElement === ultimo) {
          e.preventDefault();
          primeiro.focus();
        }
      }
    };
    document.addEventListener("keydown", handler);

    const t = setTimeout(() => firstInputRef.current?.focus(), 50);

    return () => {
      document.body.style.overflow = prev;
      document.removeEventListener("keydown", handler);
      clearTimeout(t);
      triggerRef?.current?.focus();
    };
  }, [open, onClose, triggerRef]);

  if (!open) return null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="hist-modal-title"
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.5)",
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        padding: "5vh 16px",
        zIndex: 1000,
      }}
    >
      <div
        ref={painelRef}
        onClick={(e) => e.stopPropagation()}
        style={{
          background: COLORS.bgWhite,
          borderRadius: RADIUS.lg,
          boxShadow: SHADOWS.lg,
          border: `1px solid ${COLORS.border}`,
          width: "100%",
          maxWidth: 960,
          maxHeight: "85vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}
      >
        {/* header */}
        <header
          style={{
            padding: "16px 20px",
            borderBottom: `1px solid ${COLORS.border}`,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 16,
          }}
        >
          <h2 id="hist-modal-title" style={{ fontSize: 16, margin: 0, color: COLORS.text }}>
            Histórico de validações
          </h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="Fechar histórico"
            style={{
              ...btnSecondary,
              padding: "4px 12px",
              fontSize: 12,
            }}
          >
            Fechar
          </button>
        </header>

        {/* filtros */}
        <div
          style={{
            padding: "14px 20px",
            borderBottom: `1px solid ${COLORS.border}`,
            display: "grid",
            gridTemplateColumns: "1fr 1fr 120px",
            gap: 14,
            alignItems: "end",
          }}
        >
          <div>
            <label style={baseLabel} htmlFor="hist-data-inicio">
              D-1 de
            </label>
            <input
              id="hist-data-inicio"
              ref={firstInputRef}
              type="date"
              value={filtros.dataInicio}
              onChange={(e) => setFiltros({ ...filtros, dataInicio: e.target.value })}
              style={baseInput}
            />
          </div>
          <div>
            <label style={baseLabel} htmlFor="hist-data-fim">
              D-1 até
            </label>
            <input
              id="hist-data-fim"
              type="date"
              value={filtros.dataFim}
              onChange={(e) => setFiltros({ ...filtros, dataFim: e.target.value })}
              style={baseInput}
            />
          </div>
          <div>
            <label style={baseLabel} htmlFor="hist-limite">
              Limite
            </label>
            <input
              id="hist-limite"
              type="number"
              min={1}
              max={LIMITE_MAX}
              value={filtros.limite}
              onChange={(e) =>
                setFiltros({ ...filtros, limite: Number(e.target.value) || 30 })
              }
              style={baseInput}
            />
          </div>
        </div>

        {erroRange && (
          <div style={{ padding: "10px 20px" }}>
            <div style={errorBox}>{erroRange}</div>
          </div>
        )}
        {avisoAmplo && !erroRange && (
          <div style={{ padding: "8px 20px" }}>
            <div
              style={{
                ...errorBox,
                background: COLORS.warningBg,
                color: COLORS.warningFg,
                border: `1px solid ${COLORS.warning}40`,
              }}
            >
              {avisoAmplo}
            </div>
          </div>
        )}

        {/* corpo */}
        <div style={{ flex: 1, overflowY: "auto" }}>
          {loading ? (
            <div style={{ padding: 40, display: "flex", justifyContent: "center" }}>
              <Spinner />
            </div>
          ) : erro ? (
            <div style={{ padding: 20 }}>
              <div style={errorBox}>{erro}</div>
            </div>
          ) : rows.length === 0 ? (
            <div
              style={{
                padding: 40,
                textAlign: "center",
                color: COLORS.textSecondary,
                fontSize: 13,
              }}
            >
              Nenhuma validação no intervalo selecionado.
              <br />
              Tente alargar o range de datas.
            </div>
          ) : (
            rows.map((item) => (
              <HistoricoRow
                key={item.id}
                item={item}
                onSelecionar={(id, dataD1) => {
                  onClose();
                  onSelecionar(id, dataD1);
                }}
              />
            ))
          )}
        </div>

        {/* footer */}
        <footer
          style={{
            padding: "10px 20px",
            borderTop: `1px solid ${COLORS.border}`,
            fontSize: 11,
            color: COLORS.textMuted,
            textAlign: "right",
          }}
        >
          {rows.length > 0 && `${rows.length} validação(ões) encontrada(s)`}
        </footer>
      </div>
    </div>
  );
}
