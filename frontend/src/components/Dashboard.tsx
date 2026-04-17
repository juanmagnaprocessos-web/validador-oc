import { useEffect, useRef, useState } from "react";
import {
  apiFetch,
  AuthError,
  cronRunNow,
  d1Iso,
  getConfig,
  getCronStatus,
  getHistorico,
  getResultados,
  type CronStatus,
  type HistoricoEntry,
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
import { HistoricoModal } from "./HistoricoModal";
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

type TabView = "todas" | "revisao";

export function Dashboard() {
  const [dataD1, setDataD1] = useState<string>(d1Iso());
  const [dryRun, setDryRun] = useState<boolean>(true);
  const [rodando, setRodando] = useState<boolean>(false);
  const [erro, setErro] = useState<string | null>(null);
  const [ultima, setUltima] = useState<ValidarResponse | null>(null);
  const [resultados, setResultados] = useState<OcResultado[]>([]);
  const [historico, setHistorico] = useState<HistoricoEntry[]>([]);
  const [tabView, setTabView] = useState<TabView>("todas");
  const [downloadando, setDownloadando] = useState<"html" | "excel" | null>(null);
  const [ciliaMode, setCiliaMode] = useState<string>("off");
  const [ciliaBaseUrl, setCiliaBaseUrl] = useState<string>("");
  const [modalHistoricoAberto, setModalHistoricoAberto] = useState<boolean>(false);
  const [cronStatus, setCronStatus] = useState<CronStatus | null>(null);
  const verTodasBtnRef = useRef<HTMLButtonElement>(null);

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
      const h = await getHistorico({ limite: 5 });
      setHistorico(h);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      console.error(e);
    }
  }

  async function carregarCronStatus() {
    try {
      const s = await getCronStatus();
      setCronStatus(s);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      console.error("Falha ao carregar status do CRON:", e);
    }
  }

  async function dispararCronManual() {
    try {
      await cronRunNow();
      setTimeout(() => {
        carregarHistorico();
        carregarCronStatus();
      }, 1500);
    } catch (e) {
      if (e instanceof AuthError) {
        window.location.reload();
        return;
      }
      setErro(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    carregarHistorico();
    carregarCronStatus();
    // Busca config publica para ter cilia_mode/cilia_base_url disponiveis
    // independente de o usuario ter rodado uma validacao nesta sessao.
    getConfig()
      .then((cfg) => {
        setCiliaMode(cfg.cilia_mode);
        setCiliaBaseUrl(cfg.cilia_base_url);
      })
      .catch((e) => {
        if (e instanceof AuthError) {
          window.location.reload();
          return;
        }
        console.error("Falha ao carregar config:", e);
      });
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
    if (downloadando) return;
    setErro(null);
    setDownloadando(tipo);
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
    } finally {
      setDownloadando(null);
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

  const banners = (() => {
    if (!cronStatus) return null;
    const nodes: JSX.Element[] = [];
    const ultimaFalha = cronStatus.ultima_falha;
    if (ultimaFalha) {
      // Só mostra banner se a falha é recente (últimos 2 dias)
      const updatedDate = ultimaFalha.updated_at?.slice(0, 10) ?? "";
      const limite = new Date();
      limite.setDate(limite.getDate() - 2);
      const limiteIso = limite.toISOString().slice(0, 10);
      if (updatedDate >= limiteIso) {
        nodes.push(
          <div
            key="falha"
            role="alert"
            style={{
              ...errorBox,
              marginBottom: 12,
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              gap: 12,
            }}
          >
            <div>
              <strong>CRON falhou</strong> em {ultimaFalha.data_d1} —{" "}
              {ultimaFalha.last_error || "erro desconhecido"}
            </div>
            <button
              type="button"
              onClick={dispararCronManual}
              style={{ ...btnSecondary, fontSize: 11 }}
            >
              Executar agora
            </button>
          </div>,
        );
      }
    }

    if (cronStatus.pendente_execucao) {
      const pe = cronStatus.pendente_execucao;
      nodes.push(
        <div
          key="pendente-execucao"
          role="alert"
          style={{
            ...errorBox,
            marginBottom: 12,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 12,
          }}
        >
          <div>
            <strong>CRON não executou</strong> hoje às {pe.horario_esperado} —
            sem registro para o D-1 {pe.data_d1}. App pode ter dormido (Render
            free) ou o agendador não disparou.
          </div>
          <button
            type="button"
            onClick={dispararCronManual}
            style={{ ...btnSecondary, fontSize: 11 }}
          >
            Executar agora
          </button>
        </div>,
      );
    }

    if (cronStatus.dry_runs_pendentes.length > 0) {
      nodes.push(
        <div
          key="pendentes"
          role="status"
          style={{
            padding: "10px 12px",
            background: COLORS.warningBg,
            color: COLORS.warningFg,
            border: `1px solid ${COLORS.warning}40`,
            borderRadius: RADIUS.sm,
            fontSize: 12,
            marginBottom: 12,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 12,
          }}
        >
          <div>
            Você tem <strong>{cronStatus.dry_runs_pendentes.length}</strong>{" "}
            validação(ões) automática(s) dos últimos 3 dias ainda não aplicada(s)
            no Pipefy.
          </div>
          <button
            type="button"
            onClick={() => setModalHistoricoAberto(true)}
            style={{ ...btnSecondary, fontSize: 11 }}
          >
            Ver pendentes
          </button>
        </div>,
      );
    }
    return nodes.length > 0 ? <div>{nodes}</div> : null;
  })();

  return (
    <>
      {banners}
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
          Dry run (não altera Pipefy)
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
              disabled={downloadando !== null}
              style={{
                ...btnSecondary,
                opacity: downloadando !== null ? 0.6 : 1,
                cursor: downloadando !== null ? "not-allowed" : "pointer",
              }}
              aria-label="Abrir relatorio HTML em nova aba"
              aria-busy={downloadando === "html"}
              title="Abrir relatorio completo em nova aba"
            >
              {downloadando === "html" ? "Gerando..." : "Ver HTML"}
            </button>
            <button
              type="button"
              onClick={() => baixarRelatorio(ultima.data_d1, "excel")}
              disabled={downloadando !== null}
              style={{
                ...btnSecondary,
                opacity: downloadando !== null ? 0.6 : 1,
                cursor: downloadando !== null ? "not-allowed" : "pointer",
              }}
              aria-label="Baixar relatorio Excel com cards e orfas"
              aria-busy={downloadando === "excel"}
              title="Baixar planilha com 2 abas: Cards (validacao completa) + OCs sem card no Pipefy"
            >
              {downloadando === "excel" ? "Gerando..." : "Baixar Excel"}
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
              background: tabView === "revisao" ? COLORS.dangerDim : "transparent",
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

      {/* Histórico */}
      {historico.length > 0 && (
        <section style={{ marginTop: 32 }} aria-label="Histórico de validações">
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
              marginBottom: 10,
              gap: 12,
            }}
          >
            <h2 style={{ fontSize: 16, margin: 0, color: COLORS.text }}>
              Últimas validações
            </h2>
            <button
              ref={verTodasBtnRef}
              type="button"
              onClick={() => setModalHistoricoAberto(true)}
              style={{ ...btnSecondary, fontSize: 12 }}
              aria-label="Abrir histórico completo com filtros"
            >
              Ver todas →
            </button>
          </div>
          <div style={{ ...cardPanel, padding: 8 }}>
            {historico.map((h) => (
              <div
                key={h.id}
                onClick={() => abrirHistorico(h.id, h.data_d1)}
                role="button"
                tabIndex={0}
                aria-label={`Abrir validação ${h.data_d1} com ${h.total_ocs} OCs`}
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
                <span title="Aprovadas" style={{ color: COLORS.success }}>
                  {h.aprovadas} aprov.
                </span>
                <span title="Divergências" style={{ color: COLORS.warning }}>
                  {h.divergentes} div.
                </span>
                {h.aguardando_ml != null && h.aguardando_ml > 0 && (
                  <span title="Aguardando Mercado Livre" style={{ color: COLORS.warningAmber }}>
                    {h.aguardando_ml} ML
                  </span>
                )}
                {h.ja_processadas != null && h.ja_processadas > 0 && (
                  <span title="Já processadas" style={{ color: COLORS.textMuted }}>
                    {h.ja_processadas} proc.
                  </span>
                )}
                {h.origem === "cron" && (
                  <span
                    title="Execução automática pelo CRON das 02:00"
                    style={{
                      marginLeft: "auto",
                      fontSize: 10,
                      fontWeight: 600,
                      letterSpacing: 0.5,
                      color: COLORS.primary,
                      textTransform: "uppercase",
                    }}
                  >
                    Automático
                  </span>
                )}
              </div>
            ))}
          </div>
        </section>
      )}

      <HistoricoModal
        open={modalHistoricoAberto}
        onClose={() => setModalHistoricoAberto(false)}
        onSelecionar={abrirHistorico}
        triggerRef={verTodasBtnRef}
      />


      {/* Loading global */}
      {rodando && (
        <div
          role="status"
          aria-live="polite"
          style={{
            position: "fixed",
            bottom: 24,
            right: 24,
            background: COLORS.bgWhite,
            color: COLORS.text,
            border: `1px solid ${COLORS.border}`,
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
          <Spinner size={18} color={COLORS.primary} />
          Validando OCs...
        </div>
      )}
    </>
  );
}
