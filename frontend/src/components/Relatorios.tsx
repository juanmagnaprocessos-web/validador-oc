import { useState } from "react";
import {
  baixarExcelDuplicadas,
  getRelatorioDuplicadas,
  type RelatorioDuplicadas,
} from "../api/client";
import {
  COLORS,
  FONT,
  baseInput,
  baseLabel,
  btnPrimary,
  btnSecondary,
  cardPanel,
  errorBox,
  tdStyle,
  thStyle,
} from "../styles/theme";
import { Spinner } from "./Spinner";

function fmtData(iso: string): string {
  const [a, m, d] = iso.split("-");
  return `${d}/${m}/${a}`;
}

export function Relatorios() {
  const [dataInicio, setDataInicio] = useState("");
  const [dataFim, setDataFim] = useState("");
  const [dados, setDados] = useState<RelatorioDuplicadas | null>(null);
  const [carregando, setCarregando] = useState(false);
  const [baixando, setBaixando] = useState(false);
  const [erro, setErro] = useState<string | null>(null);

  const periodoInvalido = !!dataInicio && !!dataFim && dataInicio > dataFim;

  async function gerar() {
    setCarregando(true);
    setErro(null);
    try {
      setDados(await getRelatorioDuplicadas(dataInicio || undefined, dataFim || undefined));
    } catch (e) {
      setErro(e instanceof Error ? e.message : "Erro ao gerar relatório");
    } finally {
      setCarregando(false);
    }
  }

  async function baixar() {
    setBaixando(true);
    setErro(null);
    try {
      await baixarExcelDuplicadas(dataInicio || undefined, dataFim || undefined);
    } catch (e) {
      setErro(e instanceof Error ? e.message : "Erro ao baixar Excel");
    } finally {
      setBaixando(false);
    }
  }

  const campoData = (
    rotulo: string,
    valor: string,
    onChange: (v: string) => void,
  ) => (
    <label style={{ ...baseLabel, marginBottom: 0, display: "flex", alignItems: "center", gap: 8 }}>
      {rotulo}
      <input
        type="date"
        value={valor}
        onChange={(e) => onChange(e.target.value)}
        style={{ ...baseInput, width: "auto", padding: "6px 10px" }}
        aria-label={rotulo}
      />
    </label>
  );

  return (
    <>
      <h2 style={{ fontSize: 16, fontWeight: 600, color: COLORS.text, margin: "0 0 12px" }}>
        Relatório de OCs duplicadas
      </h2>

      <section
        style={{
          ...cardPanel,
          padding: 16,
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
        }}
        aria-label="Filtros do relatório"
      >
        {campoData("Data inicial", dataInicio, setDataInicio)}
        {campoData("Data final", dataFim, setDataFim)}
        <button
          onClick={gerar}
          disabled={carregando || periodoInvalido}
          style={{ ...btnPrimary, marginLeft: "auto" }}
        >
          {carregando ? (
            <>
              <Spinner size={16} color="#ffffff" />
              Gerando...
            </>
          ) : (
            "Gerar relatório"
          )}
        </button>
        <button
          onClick={baixar}
          disabled={baixando || periodoInvalido || !dados}
          style={btnSecondary}
        >
          {baixando ? "Baixando..." : "Baixar Excel"}
        </button>
      </section>

      {periodoInvalido && (
        <div style={{ ...errorBox, marginTop: 12 }}>
          A data inicial não pode ser maior que a data final.
        </div>
      )}
      {erro && <div style={{ ...errorBox, marginTop: 12 }}>{erro}</div>}

      {dados && (
        <section style={{ ...cardPanel, marginTop: 16, overflow: "hidden" }}>
          <div
            style={{
              padding: "12px 14px",
              fontSize: 12,
              color: COLORS.textSecondary,
              borderBottom: `1px solid ${COLORS.border}`,
            }}
          >
            <strong style={{ color: COLORS.text }}>{dados.total_pecas}</strong> peça(s)
            duplicada(s) em <strong style={{ color: COLORS.text }}>{dados.total_ocs}</strong>{" "}
            OC(s)
            {(dados.data_inicio || dados.data_fim) && (
              <>
                {" "}
                · D-1 de {dados.data_inicio ? fmtData(dados.data_inicio) : "início"} a{" "}
                {dados.data_fim ? fmtData(dados.data_fim) : "hoje"}
              </>
            )}
          </div>

          {dados.total_pecas === 0 ? (
            <div style={{ padding: 24, textAlign: "center", color: COLORS.textMuted, fontSize: 13 }}>
              Nenhuma duplicidade encontrada no período.
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={thStyle}>D-1</th>
                    <th style={thStyle}>OC</th>
                    <th style={thStyle}>Placa</th>
                    <th style={thStyle}>Peça</th>
                    <th style={thStyle}>Tipo</th>
                    <th style={thStyle}>Devolução</th>
                    <th style={thStyle}>Fornecedor</th>
                    <th style={thStyle}>OC anterior</th>
                  </tr>
                </thead>
                <tbody>
                  {dados.linhas.map((l, i) => (
                    <tr
                      key={`${l.data_d1}|${l.id_pedido}|${l.peca}|${i}`}
                      style={{ borderBottom: `1px solid ${COLORS.border}` }}
                    >
                      <td style={tdStyle}>{fmtData(l.data_d1)}</td>
                      <td style={{ ...tdStyle, fontFamily: FONT.mono }}>{l.id_pedido}</td>
                      <td style={{ ...tdStyle, fontFamily: FONT.mono }}>{l.placa || "—"}</td>
                      <td style={tdStyle}>{l.peca || "—"}</td>
                      <td style={tdStyle}>{l.tipo}</td>
                      <td style={tdStyle}>{l.devolucao || "—"}</td>
                      <td style={tdStyle}>{l.fornecedor || "—"}</td>
                      <td style={{ ...tdStyle, fontSize: 12 }}>
                        {l.oc_anterior ? (
                          <>
                            {l.link_oc_anterior ? (
                              <a href={l.link_oc_anterior} target="_blank" rel="noreferrer">
                                {l.oc_anterior}
                              </a>
                            ) : (
                              <span style={{ fontFamily: FONT.mono }}>{l.oc_anterior}</span>
                            )}
                            {l.data_anterior && <> · {fmtData(l.data_anterior)}</>}
                            {l.fornecedor_anterior && <> · {l.fornecedor_anterior}</>}
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}
    </>
  );
}
