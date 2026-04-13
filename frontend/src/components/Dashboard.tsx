import { useEffect, useState } from "react";
import {
  apiFetch,
  AuthError,
  d1Iso,
  getHistorico,
  getResultados,
  OcResultado,
  urlRelatorioExcel,
  urlRelatorioHtml,
  validar,
  ValidarResponse,
} from "../api/client";
import { SummaryCards } from "./Cards";
import { ResultadosTable } from "./ResultadosTable";
import { SummaryChart } from "./SummaryChart";
import { Spinner } from "./Spinner";
import {
  COLORS,
  SHADOWS,
  RADIUS,
  baseInput,
  baseLabel,
  btnPrimary,
  btnSecondary,
  errorBox,
  cardPanel,
} from "../styles/theme";

interface HistoricoItem {
  id: number;
  data_d1: string;
  total_ocs: number;
  aprovadas: number;
  divergentes: number;
  aguardando_ml?: number | null;
  ja_processadas?: number | null;
}

type TabView = "todas" | "revisao";

export function Dashboard() {
  const [dataD1, setDataD1] = useState<string>(d1Iso());
  const [dryRun, setDryRun] = useState<boolean>(true);
  const [rodando, setRodando] = useState<boolean>(false);
  const [erro, setErro] = useState<string | null>(null);
  const [ultima, setUltima] = useState<ValidarResponse | null>(null);
  const [resultados, setResultados] = useState<OcResultado[]>([]);
  const [historico, setHistorico] = useState<HistoricoItem[]>([]);
  const [tabView, setTabView] = useState<TabView>("todas");
  const [ciliaMode, setCiliaMode] = useState<string>("off");
  const [ciliaBaseUrl, setCiliaBaseUrl] = useState<string>("");

  // Filtrar resultados para revisao: OCs que requerem atenção do analista
  const resultadosRevisao = resultados.filter((r) => {
    const temReincidencia = r.reincidencia && r.reincidencia !== "\u2014" && r.reincidencia !== "--";
    const temDivergencia = r.status === "divergencia" || r.status === "bloqueada";
    const poucasCotacoes = r.qtd_cotacoes != null && r.qtd_cotacoes < 3;
    const pecaDuplicada = r.peca_duplicada != null && r.peca_duplicada !== "Nao" && r.peca_duplicada !== "\u2014";
    const semCard = !r.card_pipefy_id && !r.card_pipefy_link;
    return temDivergencia || temReincidencia || poucasCotacoes || pecaDuplicada || semCard;
  });

  async function carregarHistorico() {
    try {
      const h = await getHistorico(10);
      setHistorico(h);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      console.error(e);
    }
  }

  useEffect(() => {
    carregarHistorico();
  }, []);

  async function rodar() {
    setErro(null);
    setRodando(true);
    try {
      const r = await validar(dataD1, dryRun);
      setUltima(r);
      if (r.cilia_mode) setCiliaMode(r.cilia_mode);
      if (r.cilia_base_url) setCiliaBaseUrl(r.cilia_base_url);
      const res = await getResultados(r.validacao_id);
      setResultados(res);
      await carregarHistorico();
    } catch (e: unknown) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      setErro(e instanceof Error ? e.message : String(e));
    } finally {
      setRodando(false);
    }
  }

  async function baixarRelatorio(data: string, tipo: "html" | "excel") {
    setErro(null);
    try {
      const url =
        tipo === "html" ? urlRelatorioHtml(data) : urlRelatorioExcel(data);
      const resp = await apiFetch(url);
      if (!resp.ok) {
        throw new Error(`Erro ${resp.status} ao gerar relatorio`);
      }
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
    } catch (e: unknown) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  async function abrirHistorico(validacaoId: number, data: string) {
    try {
      const res = await getResultados(validacaoId);
      setResultados(res);
      setUltima({
        validacao_id: validacaoId,
        data_d1: data,
        total: res.length,
        aprovadas: res.filter((r) => r.status === "aprovada").length,
        divergentes: res.filter((r) => r.status === "divergencia").length,
        bloqueadas: res.filter((r) => r.status === "bloqueada").length,
        aguardando_ml: res.filter((r) => r.status === "aguardando_ml").length,
        ja_processadas: res.filter((r) => r.status === "ja_processada").length,
        dry_run: true,
        relatorio_html: `${data}_validacao.html`,
        relatorio_xlsx: `${data}_validacao.xlsx`,
      });
      setDataD1(data);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      console.error(e);
    }
  }

  return (
    <>
      {/* Barra de controles */}
      <section
        style={{
          ...cardPanel,
          padding: 16,
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
        }}
        aria-label="Controles de validacao"
      >
        <label style={{ ...baseLabel, marginBottom: 0, display: "flex", alignItems: "center", gap: 8 }}>
          Data (D-1)
          <input
            type="date"
            value={dataD1}
            onChange={(e) => setDataD1(e.target.value)}
            disabled={rodando}
            style={{
              ...baseInput,
              width: "auto",
              padding: "6px 10px",
            }}
            aria-label="Selecionar data D-1 para validacao"
          />
        </label>

        <label
          style={{
            fontSize: 13,
            color: COLORS.textSecondary,
            display: "flex",
            alignItems: "center",
            gap: 6,
            cursor: "pointer",
          }}
        >
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            disabled={rodando}
          />
          Dry run (nao altera Pipefy)
        </label>

        <button
          onClick={rodar}
          disabled={rodando}
          style={{ ...btnPrimary, marginLeft: "auto" }}
          aria-label={rodando ? "Validacao em andamento" : "Iniciar validacao das OCs do D-1"}
        >
          {rodando ? (
            <>
              <Spinner size={16} color="#ffffff" />
              Executando...
            </>
          ) : (
            "Puxar dados do D-1"
          )}
        </button>

        {ultima && (
          <>
            <button
              type="button"
              onClick={() => baixarRelatorio(ultima.data_d1, "html")}
              style={btnSecondary}
              aria-label="Abrir relatorio HTML em nova aba"
              title="Abrir relatorio completo em nova aba"
            >
              Ver HTML
            </button>
            <button
              type="button"
              onClick={() => baixarRelatorio(ultima.data_d1, "excel")}
              style={btnSecondary}
              aria-label="Baixar relatorio Excel com cards e orfas"
              title="Baixar planilha com 2 abas: Cards (validacao completa) + OCs sem card no Pipefy"
            >
              Baixar Excel
            </button>
          </>
        )}
      </section>

      {/* Erro */}
      {erro && (
        <div style={{ ...errorBox, marginTop: 16 }} role="alert">
          <strong>Erro:</strong> {erro}
        </div>
      )}

      {/* Cards + Grafico */}
      {ultima && (
        <section aria-label="Resumo da validacao" style={{ marginTop: 24 }}>
          <SummaryCards
            total={ultima.total}
            aprovadas={ultima.aprovadas}
            divergentes={ultima.divergentes}
            bloqueadas={ultima.bloqueadas ?? 0}
            aguardandoMl={ultima.aguardando_ml ?? 0}
            jaProcessadas={ultima.ja_processadas ?? 0}
          />
          <div
            style={{
              ...cardPanel,
              padding: 20,
              marginTop: 16,
            }}
          >
            <SummaryChart
              aprovadas={ultima.aprovadas}
              divergentes={ultima.divergentes}
              bloqueadas={ultima.bloqueadas ?? 0}
              aguardandoMl={ultima.aguardando_ml ?? 0}
              jaProcessadas={ultima.ja_processadas ?? 0}
            />
          </div>
        </section>
      )}

      {/* Abas: Todas OCs / Revisao Final */}
      {resultados.length > 0 && (
        <div style={{ marginTop: 24, display: "flex", gap: 0, borderBottom: `2px solid ${COLORS.borderLight}` }}>
          <button
            onClick={() => setTabView("todas")}
            style={{
              ...btnSecondary,
              borderRadius: "6px 6px 0 0",
              border: "none",
              borderBottom: tabView === "todas" ? `2px solid ${COLORS.primary}` : "2px solid transparent",
              background: tabView === "todas" ? COLORS.bgWhite : "transparent",
              color: tabView === "todas" ? COLORS.primary : COLORS.textSecondary,
              fontWeight: tabView === "todas" ? 600 : 400,
            }}
          >
            Todas OCs ({resultados.length})
          </button>
          <button
            onClick={() => setTabView("revisao")}
            style={{
              ...btnSecondary,
              borderRadius: "6px 6px 0 0",
              border: "none",
              borderBottom: tabView === "revisao" ? `2px solid ${COLORS.danger}` : "2px solid transparent",
              background: tabView === "revisao" ? "#fff5f5" : "transparent",
              color: tabView === "revisao" ? COLORS.danger : COLORS.textSecondary,
              fontWeight: tabView === "revisao" ? 600 : 400,
            }}
          >
            Revisao Final ({resultadosRevisao.length})
            {resultadosRevisao.length > 0 && (
              <span
                style={{
                  marginLeft: 6,
                  background: COLORS.danger,
                  color: "#fff",
                  padding: "1px 6px",
                  borderRadius: 10,
                  fontSize: 10,
                  fontWeight: 700,
                }}
              >
                !
              </span>
            )}
          </button>
        </div>
      )}

      {/* Tabela de resultados */}
      <section style={{ marginTop: 4 }} aria-label="Resultados da validacao">
        <ResultadosTable
          resultados={tabView === "revisao" ? resultadosRevisao : resultados}
          ciliaMode={ciliaMode}
          ciliaBaseUrl={ciliaBaseUrl}
        />
      </section>

      {/* Historico */}
      {historico.length > 0 && (
        <section style={{ marginTop: 32 }} aria-label="Historico de validacoes">
          <h2 style={{ fontSize: 16, marginBottom: 10, color: COLORS.text }}>
            Ultimas validacoes
          </h2>
          <div style={{ ...cardPanel, padding: 8 }}>
            {historico.map((h) => (
              <div
                key={h.id}
                onClick={() => abrirHistorico(h.id, h.data_d1)}
                role="button"
                tabIndex={0}
                aria-label={`Abrir validacao ${h.data_d1} com ${h.total_ocs} OCs`}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    abrirHistorico(h.id, h.data_d1);
                  }
                }}
                style={{
                  padding: "10px 14px",
                  display: "flex",
                  gap: 16,
                  fontSize: 13,
                  cursor: "pointer",
                  borderRadius: RADIUS.sm,
                  alignItems: "center",
                  transition: "background 100ms ease",
                }}
                onMouseEnter={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = COLORS.bg)
                }
                onMouseLeave={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = "transparent")
                }
              >
                <span style={{ color: COLORS.textSecondary, fontFamily: "monospace", fontSize: 12 }}>
                  #{h.id}
                </span>
                <span style={{ fontWeight: 600 }}>{h.data_d1}</span>
                <span>{h.total_ocs} OCs</span>
                <span style={{ color: COLORS.success }}>{h.aprovadas} aprov</span>
                <span style={{ color: COLORS.warning }}>{h.divergentes} div</span>
                {h.aguardando_ml != null && h.aguardando_ml > 0 && (
                  <span style={{ color: COLORS.warningAmber }}>{h.aguardando_ml} ML</span>
                )}
                {h.ja_processadas != null && h.ja_processadas > 0 && (
                  <span style={{ color: COLORS.textMuted }}>{h.ja_processadas} proc</span>
                )}
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Loading global */}
      {rodando && (
        <div
          role="status"
          aria-live="polite"
          style={{
            position: "fixed",
            bottom: 24,
            right: 24,
            background: COLORS.text,
            color: "#ffffff",
            padding: "12px 20px",
            borderRadius: RADIUS.md,
            boxShadow: SHADOWS.lg,
            display: "flex",
            alignItems: "center",
            gap: 10,
            fontSize: 13,
            fontWeight: 500,
            zIndex: 50,
          }}
        >
          <Spinner size={18} color="#ffffff" />
          Validando OCs...
        </div>
      )}
    </>
  );
}
