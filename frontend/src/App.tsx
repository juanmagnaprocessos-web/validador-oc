import { useEffect, useState } from "react";
import {
  d1Iso,
  getHistorico,
  getResultados,
  OcResultado,
  urlRelatorioExcel,
  urlRelatorioHtml,
  validar,
  ValidarResponse,
} from "./api/client";
import { SummaryCards } from "./components/Cards";
import { ResultadosTable } from "./components/ResultadosTable";

export default function App() {
  const [dataD1, setDataD1] = useState<string>(d1Iso());
  const [dryRun, setDryRun] = useState<boolean>(true);
  const [rodando, setRodando] = useState<boolean>(false);
  const [erro, setErro] = useState<string | null>(null);
  const [ultima, setUltima] = useState<ValidarResponse | null>(null);
  const [resultados, setResultados] = useState<OcResultado[]>([]);
  const [historico, setHistorico] = useState<
    {
      id: number;
      data_d1: string;
      total_ocs: number;
      aprovadas: number;
      divergentes: number;
      aguardando_ml?: number | null;
      ja_processadas?: number | null;
    }[]
  >([]);

  async function carregarHistorico() {
    try {
      const h = await getHistorico(10);
      setHistorico(h);
    } catch (e) {
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
      const res = await getResultados(r.validacao_id);
      setResultados(res);
      await carregarHistorico();
    } catch (e: unknown) {
      setErro(e instanceof Error ? e.message : String(e));
    } finally {
      setRodando(false);
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
      console.error(e);
    }
  }

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: 24 }}>
      <header
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <div>
          <h1 style={{ margin: 0, fontSize: 24 }}>Validador OC — Magna Proteção</h1>
          <div style={{ color: "#5a6c7f", fontSize: 13 }}>
            Automação da validação diária de Ordens de Compra
          </div>
        </div>
      </header>

      {/* Barra de ações */}
      <div
        style={{
          background: "white",
          borderRadius: 8,
          padding: 16,
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
          boxShadow: "0 1px 3px rgba(0,0,0,.06)",
        }}
      >
        <label style={{ fontSize: 13, color: "#5a6c7f" }}>
          Data (D-1)
          <input
            type="date"
            value={dataD1}
            onChange={(e) => setDataD1(e.target.value)}
            disabled={rodando}
            style={{
              marginLeft: 8,
              padding: "6px 10px",
              border: "1px solid #d1d5db",
              borderRadius: 6,
              fontSize: 14,
            }}
          />
        </label>

        <label style={{ fontSize: 13, color: "#5a6c7f" }}>
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            disabled={rodando}
          />
          {" "}
          Dry run (não altera Pipefy)
        </label>

        <button
          onClick={rodar}
          disabled={rodando}
          style={{
            marginLeft: "auto",
            background: "#2563eb",
            color: "white",
            border: 0,
            borderRadius: 6,
            padding: "10px 20px",
            fontSize: 14,
            fontWeight: 600,
          }}
        >
          {rodando ? "Executando..." : "🔄 Puxar dados do D-1"}
        </button>

        {ultima && (
          <>
            <a
              href={urlRelatorioHtml(ultima.data_d1)}
              target="_blank"
              rel="noreferrer"
              style={btn}
            >
              Ver HTML
            </a>
            <a href={urlRelatorioExcel(ultima.data_d1)} style={btn}>
              Baixar Excel
            </a>
          </>
        )}
      </div>

      {erro && (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            background: "#fecaca",
            color: "#991b1b",
            borderRadius: 6,
          }}
        >
          <strong>Erro:</strong> {erro}
        </div>
      )}

      {ultima && (
        <SummaryCards
          total={ultima.total}
          aprovadas={ultima.aprovadas}
          divergentes={ultima.divergentes}
          bloqueadas={ultima.bloqueadas ?? 0}
          aguardandoMl={ultima.aguardando_ml ?? 0}
          jaProcessadas={ultima.ja_processadas ?? 0}
        />
      )}

      <ResultadosTable resultados={resultados} />

      {historico.length > 0 && (
        <div style={{ marginTop: 32 }}>
          <h2 style={{ fontSize: 16, marginBottom: 8 }}>Últimas validações</h2>
          <div
            style={{
              background: "white",
              borderRadius: 8,
              padding: 8,
              boxShadow: "0 1px 3px rgba(0,0,0,.06)",
            }}
          >
            {historico.map((h) => (
              <div
                key={h.id}
                onClick={() => abrirHistorico(h.id, h.data_d1)}
                style={{
                  padding: "8px 12px",
                  display: "flex",
                  gap: 16,
                  fontSize: 13,
                  cursor: "pointer",
                  borderRadius: 4,
                }}
                onMouseEnter={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = "#f4f6fa")
                }
                onMouseLeave={(e) =>
                  ((e.currentTarget as HTMLDivElement).style.background = "transparent")
                }
              >
                <span style={{ color: "#5a6c7f" }}>#{h.id}</span>
                <span style={{ fontWeight: 600 }}>{h.data_d1}</span>
                <span>{h.total_ocs} OCs</span>
                <span style={{ color: "#17a34a" }}>{h.aprovadas} aprov</span>
                <span style={{ color: "#ea580c" }}>{h.divergentes} div</span>
                {h.aguardando_ml != null && h.aguardando_ml > 0 && (
                  <span style={{ color: "#d97706" }}>{h.aguardando_ml} ML</span>
                )}
                {h.ja_processadas != null && h.ja_processadas > 0 && (
                  <span style={{ color: "#6b7280" }}>{h.ja_processadas} proc</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

const btn: React.CSSProperties = {
  background: "#f3f4f6",
  color: "#1a2332",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  padding: "8px 14px",
  fontSize: 13,
  fontWeight: 500,
  textDecoration: "none",
};
