import React, { useState, useMemo, useEffect, Fragment } from "react";
import { OcResultado, DivergenciaCompleta } from "../api/client";
import { COLORS, cardPanel, thStyle, tdStyle, btnSecondary, RADIUS } from "../styles/theme";

interface Props {
  resultados: OcResultado[];
  ciliaMode?: string;
  ciliaBaseUrl?: string;
}

const STATUS_LABEL: Record<
  string,
  { text: string; bg: string; fg: string }
> = {
  aprovada: { text: "Aprovada", bg: COLORS.successBg, fg: COLORS.successFg },
  divergencia: { text: "Divergencia", bg: COLORS.warningBg, fg: COLORS.warningFg },
  bloqueada: { text: "Bloqueada", bg: COLORS.errorBg, fg: COLORS.errorFg },
  aguardando_ml: { text: "ML -- Manual", bg: COLORS.warningBg, fg: COLORS.warningFg },
  ja_processada: { text: "Ja processada", bg: "rgba(145, 152, 165, 0.15)", fg: COLORS.textSecondary },
  sem_card_pipefy: { text: "Sem card Pipefy", bg: "rgba(217, 70, 239, 0.12)", fg: "#e9a3ff" },
};

const SEV_COLORS: Record<string, { bg: string; fg: string; icon: string }> = {
  erro: { bg: COLORS.errorBg, fg: COLORS.errorFg, icon: "\u26d4" },
  alerta: { bg: COLORS.warningBg, fg: COLORS.warningFg, icon: "\u26a0\ufe0f" },
  info: { bg: "rgba(59, 130, 246, 0.12)", fg: "#93c5fd", icon: "\u2139\ufe0f" },
};

type SortCol = "id_pedido" | "placa" | "fornecedor" | "comprador" | "valor_club" | "valor_pdf" | "qtd_cotacoes" | "status" | "reincidencia";
type SortDir = "asc" | "desc";

const PAGE_SIZE = 20;

function fmtMoney(v: number | null) {
  if (v == null) return "--";
  return v.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

// ============================================================
// Painel de detalhes expandido de uma OC
// ============================================================
function ActionGuidanceBanner({ r }: { r: OcResultado }) {
  const divR2 = (r.divergencias_json ?? []).filter((d) => d.regra === "R2");
  const hasReincNoReturn = divR2.some((d) => d.dados?.sem_devolucao && !d.dados?.tem_devolucao_peca && !d.dados?.tem_devolucao_outra_peca);
  const hasReincWithReturn = divR2.length > 0 && divR2.every((d) => d.dados?.tem_devolucao_peca);
  const lowQuotes = r.qtd_cotacoes != null && r.qtd_cotacoes < 3;

  if (hasReincNoReturn) {
    return (
      <div style={{
        padding: "12px 16px", marginBottom: 14, borderRadius: RADIUS.sm,
        background: COLORS.primary, color: "#ffffff", fontSize: 15, fontWeight: 700,
        display: "flex", alignItems: "center", gap: 10,
        boxShadow: "0 2px 14px rgba(239,68,68,.35)",
      }}>
        <span style={{ fontSize: 22 }}>{"\u26d4"}</span>
        <div>
          <div>RECUSAR -- Sem card de devolucao no Pipefy</div>
          <div style={{ fontSize: 12, fontWeight: 400, opacity: 0.9, marginTop: 2 }}>
            Reincidencia detectada sem devolucao confirmada. Sinalizar ao comprador.
          </div>
        </div>
      </div>
    );
  }
  if (hasReincWithReturn) {
    return (
      <div style={{
        padding: "12px 16px", marginBottom: 14, borderRadius: RADIUS.sm,
        background: COLORS.success, color: "#0a0b0e", fontSize: 15, fontWeight: 700,
        display: "flex", alignItems: "center", gap: 10,
        boxShadow: "0 2px 14px rgba(34,197,94,.25)",
      }}>
        <span style={{ fontSize: 22 }}>{"\u2705"}</span>
        <div>
          <div>APROVAR -- Devolucao confirmada</div>
          <div style={{ fontSize: 12, fontWeight: 400, opacity: 0.9, marginTop: 2 }}>
            Todas as reincidencias possuem card de devolucao associado.
          </div>
        </div>
      </div>
    );
  }
  if (!r.card_pipefy_id && !r.card_pipefy_link) {
    return (
      <div style={{
        padding: "12px 16px", marginBottom: 14, borderRadius: RADIUS.sm,
        background: "#7c3aed", color: "#ffffff", fontSize: 15, fontWeight: 700,
        display: "flex", alignItems: "center", gap: 10,
        boxShadow: "0 2px 14px rgba(124,58,237,.35)",
      }}>
        <span style={{ fontSize: 22 }}>{"\ud83d\udccc"}</span>
        <div>
          <div>SEM CARD NO PIPEFY</div>
          <div style={{ fontSize: 12, fontWeight: 400, opacity: 0.9, marginTop: 2 }}>
            Esta OC nao possui card vinculado no Pipefy. Verificar se o card foi criado e associar manualmente.
          </div>
        </div>
      </div>
    );
  }
  if (lowQuotes) {
    return (
      <div style={{
        padding: "12px 16px", marginBottom: 14, borderRadius: RADIUS.sm,
        background: COLORS.warning, color: "#0a0b0e", fontSize: 15, fontWeight: 700,
        display: "flex", alignItems: "center", gap: 10,
        boxShadow: "0 2px 14px rgba(245,158,11,.25)",
      }}>
        <span style={{ fontSize: 22 }}>{"\u26a0\ufe0f"}</span>
        <div>
          <div>ATENCAO -- Menos de 3 cotacoes</div>
          <div style={{ fontSize: 12, fontWeight: 400, opacity: 0.9, marginTop: 2 }}>
            Apenas {r.qtd_cotacoes} cotacao(oes) encontrada(s). Minimo exigido: 3.
          </div>
        </div>
      </div>
    );
  }
  return null;
}

function DetalheOC({ r, ciliaMode, ciliaBaseUrl }: { r: OcResultado; ciliaMode?: string; ciliaBaseUrl?: string }) {
  const divergencias = r.divergencias_json ?? [];
  const produtos = r.produtos_json ?? [];
  const divR2 = divergencias.filter((d) => d.regra === "R2");
  const divOutras = divergencias.filter((d) => d.regra !== "R2");

  // Mapear quais pecas sao reincidentes
  const pecasReincidentes = new Set(
    divR2
      .filter((d) => d.dados?.chave_produto)
      .map((d) => d.dados.chave_produto!)
  );

  return (
    <div
      style={{
        padding: "20px 24px",
        background: COLORS.bg,
        borderTop: `2px solid ${COLORS.primary}`,
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: 20,
      }}
    >
      {/* Action guidance banner spans full width */}
      <div style={{ gridColumn: "1 / -1" }}>
        <ActionGuidanceBanner r={r} />
      </div>
      {/* ---- COLUNA ESQUERDA: Informacoes Gerais + Pecas ---- */}
      <div>
        {/* Informacoes Gerais */}
        <SectionTitle>Informacoes da OC</SectionTitle>
        <InfoGrid>
          <InfoItem label="N. OC" value={r.id_pedido} />
          <InfoItem
            label="Placa"
            value={<PlacaCell placa={r.placa} />}
            hint={
              !r.placa
                ? "Esta OC foi gerada no Club sem placa associada. Verificar manualmente no card do Pipefy ou confirmar com o comprador."
                : undefined
            }
          />
          <InfoItem label="Fornecedor" value={r.fornecedor ?? "--"} />
          <InfoItem label="Comprador" value={r.comprador ?? "--"} />
          <InfoItem label="Forma Pagamento" value={r.forma_pagamento_canonica ?? r.forma_pagamento ?? "--"} />
          <InfoItem label="Cotacoes" value={String(r.qtd_cotacoes ?? 0)} highlight={r.qtd_cotacoes != null && r.qtd_cotacoes < 3 ? "erro" : undefined} />
        </InfoGrid>

        {/* Valores */}
        <SectionTitle>Valores</SectionTitle>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          <ValueCard label="Club" value={fmtMoney(r.valor_club)} highlight={r.valor_club != null && r.valor_pdf != null && r.valor_club !== r.valor_pdf} />
          <ValueCard label="PDF" value={fmtMoney(r.valor_pdf)} highlight={r.valor_club != null && r.valor_pdf != null && r.valor_club !== r.valor_pdf} />
          <ValueCard label="Card Pipefy" value={fmtMoney(r.valor_card)} />
          <ValueCard label="Cilia" value={fmtMoney(r.valor_cilia)} />
        </div>

        {/* Links */}
        <SectionTitle>Links</SectionTitle>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {r.card_pipefy_link && (
            <LinkBadge href={r.card_pipefy_link} label="Card Pipefy" color={COLORS.primary} />
          )}
          {r.cancelamento_card_id && (
            <LinkBadge
              href={`https://app.pipefy.com/pipes/305587531#cards/${r.cancelamento_card_id}`}
              label="Card Cancelamento"
              color={COLORS.danger}
            />
          )}
          {ciliaMode && ciliaMode !== "off" && ciliaBaseUrl && (
            <LinkBadge
              href={`${ciliaBaseUrl}/users/sign_in`}
              label="Verificar no Cilia"
              color="#6d28d9"
            />
          )}
          <LinkBadge
            href="https://painel.clubdacotacao.com.br/relatorios/"
            label="Consultar no Club"
            color="#ea580c"
          />
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              navigator.clipboard.writeText(r.id_pedido);
            }}
            style={{
              display: "inline-flex", alignItems: "center", gap: 4,
              padding: "4px 10px", background: COLORS.bgHover, color: COLORS.text,
              borderRadius: 4, fontSize: 11, fontWeight: 500, border: `1px solid ${COLORS.border}`,
              cursor: "pointer",
            }}
            title="Copiar numero da OC"
          >
            Copiar N. OC
          </button>
          {r.placa && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                navigator.clipboard.writeText(r.placa!);
              }}
              style={{
                display: "inline-flex", alignItems: "center", gap: 4,
                padding: "4px 10px", background: COLORS.bgHover, color: COLORS.text,
                borderRadius: 4, fontSize: 11, fontWeight: 500, border: `1px solid ${COLORS.border}`,
                cursor: "pointer",
              }}
              title="Copiar placa"
            >
              Copiar Placa
            </button>
          )}
        </div>

        {/* Lista de Pecas */}
        <SectionTitle>
          Pecas da OC ({produtos.length})
          {r.qtd_cotacoes != null && r.qtd_cotacoes < 3 && (
            <span style={{ color: COLORS.danger, fontSize: 11, marginLeft: 8 }}>
              ATENCAO: Apenas {r.qtd_cotacoes} cotacao(oes) — minimo 3
            </span>
          )}
        </SectionTitle>
        {r.produtos_json === null ? (
          <div style={{
            padding: "14px 16px", background: COLORS.warningBg, border: `1px solid rgba(245,158,11,0.3)`,
            borderRadius: RADIUS.sm, color: COLORS.warningFg, fontSize: 13, textAlign: "center",
          }}>
            Dados de pecas nao carregados -- execute nova validacao
          </div>
        ) : produtos.length > 0 ? (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: COLORS.bgHover }}>
                <th style={{ ...thSmall, width: 24 }}></th>
                <th style={thSmall}>Descricao</th>
                <th style={{ ...thSmall, textAlign: "right" }}>Qtd</th>
                <th style={{ ...thSmall, textAlign: "center" }} title="Cotacoes: quantos fornecedores cotaram esta peca">Cot.</th>
                <th style={{ ...thSmall, textAlign: "right" }}>Valor Unit.</th>
                <th style={{ ...thSmall, textAlign: "right" }}>Valor Total</th>
                <th style={{ ...thSmall, textAlign: "center" }}>Reincidencia</th>
              </tr>
            </thead>
            <tbody>
              {produtos.map((p, i) => {
                const _ean = (p.ean || "").trim();
                const _cod = (p.cod_interno || "").trim();
                const _desc = (p.descricao || "").trim().toLowerCase();
                const chave = _ean ? `ean:${_ean}` : _cod ? `cod:${_cod}` : _desc ? `desc:${_desc}` : "";
                const isReincidente = pecasReincidentes.has(chave);
                const divPeca = divR2.find((d) => d.dados?.chave_produto === chave);
                const rowKey = chave || `idx-${i}`;
                return (
                  <tr
                    key={rowKey}
                    style={{
                      borderTop: `1px solid ${COLORS.border}`,
                      background: isReincidente ? COLORS.errorBg : i % 2 === 0 ? COLORS.bgWhite : COLORS.bg,
                    }}
                  >
                    <td style={{ padding: "6px 4px", textAlign: "center" }}>
                      {isReincidente ? (
                        <span title="Reincidencia detectada" style={{ fontSize: 14 }}>
                          {divPeca?.dados?.tem_devolucao_peca ? "\u2705" : "\u274c"}
                        </span>
                      ) : (
                        <span style={{ color: COLORS.success, fontSize: 14 }}>--</span>
                      )}
                    </td>
                    <td style={{ padding: "6px 8px", fontWeight: isReincidente ? 600 : 400 }}>
                      {p.descricao || "--"}
                    </td>
                    <td style={{ padding: "6px 8px", textAlign: "right" }}>{p.quantidade}</td>
                    {(() => {
                      // Valor por peca (preferencial); fallback para global da OC.
                      const cotacoesPeca =
                        p.qtd_cotacoes_peca ?? r.qtd_cotacoes ?? null;
                      const abaixo = (cotacoesPeca ?? 0) < 3;
                      return (
                        <td style={{
                          padding: "6px 8px", textAlign: "center", fontSize: 11,
                          fontWeight: abaixo ? 700 : 400,
                          color: abaixo ? COLORS.danger : COLORS.successFg,
                          background: abaixo ? COLORS.errorBg : "transparent",
                        }}>
                          {cotacoesPeca ?? "--"}
                          {(p.qtd_ocs_com_peca ?? 0) > 1 && (
                            <div style={{ fontSize: 9, color: COLORS.danger, fontWeight: 700 }}>
                              {p.qtd_ocs_com_peca} OCs
                            </div>
                          )}
                        </td>
                      );
                    })()}
                    <td style={{ padding: "6px 8px", textAlign: "right", fontSize: 11 }}>
                      {fmtMoney(p.valor_unitario ?? null)}
                    </td>
                    <td style={{ padding: "6px 8px", textAlign: "right", fontSize: 11 }}>
                      {fmtMoney(p.valor_total ?? null)}
                    </td>
                    <td style={{ padding: "6px 8px", textAlign: "center", fontSize: 11 }}>
                      {isReincidente && divPeca ? (
                        <ReincidenciaBadge div={divPeca} />
                      ) : (
                        <span style={{ color: COLORS.textMuted }}>OK</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <div style={{ color: COLORS.textMuted, fontSize: 12 }}>
            Sem dados de pecas disponiveis
          </div>
        )}
      </div>

      {/* ---- COLUNA DIREITA: Reincidencias + Divergencias ---- */}
      <div>
        {/* Reincidencias (R2 cross-time) — DESTAQUE PRINCIPAL */}
        {divR2.length > 0 && (
          <>
            <SectionTitle color={COLORS.danger}>
              Reincidencias Detectadas ({divR2.length})
            </SectionTitle>
            {divR2.map((d, i) => (
              <ReincidenciaCard
                key={`${d.regra}-${d.dados?.chave_produto ?? d.titulo}-${i}`}
                div={d}
              />
            ))}
          </>
        )}

        {/* Status de Reincidencia/Cancelamento */}
        <SectionTitle>Status Geral</SectionTitle>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
          <StatusBadge
            label="Reincidencia"
            value={r.reincidencia ?? "\u2014"}
            type={
              !r.reincidencia || r.reincidencia === "\u2014"
                ? "ok"
                : r.reincidencia.includes("sem_devolucao")
                  ? "error"
                  : "warn"
            }
          />
          <StatusBadge
            label="Cancelamento"
            value={r.cancelamento ?? "\u2014"}
            type={
              !r.cancelamento || r.cancelamento === "\u2014" ? "ok" : "warn"
            }
          />
          <StatusBadge
            label="Peca Duplicada"
            value={r.peca_duplicada}
            type={r.peca_duplicada === "Nao" ? "ok" : "error"}
          />
        </div>

        {/* Outras Divergencias */}
        {divOutras.length > 0 && (
          <>
            <SectionTitle>Outras Divergencias ({divOutras.length})</SectionTitle>
            {divOutras.map((d, i) => (
              <DivergenciaCard
                key={`${d.regra}-${d.dados?.chave_produto ?? d.titulo}-${i}`}
                div={d}
              />
            ))}
          </>
        )}

        {/* Se nao tem nenhuma divergencia */}
        {divergencias.length === 0 && (
          <div
            style={{
              padding: 16,
              background: COLORS.successBg,
              borderRadius: RADIUS.sm,
              color: COLORS.successFg,
              fontSize: 13,
              textAlign: "center",
            }}
          >
            Nenhuma divergencia encontrada — OC aprovada
          </div>
        )}
      </div>
    </div>
  );
}

// ============================================================
// Subcomponentes de UI
// ============================================================

function SectionTitle({ children, color }: { children: React.ReactNode; color?: string }) {
  return (
    <h4
      style={{
        fontSize: 13,
        textTransform: "uppercase",
        letterSpacing: 0.5,
        color: color ?? COLORS.textSecondary,
        margin: "16px 0 8px",
        fontWeight: 700,
        borderBottom: `2px solid ${color ?? COLORS.borderLight}`,
        paddingBottom: 6,
      }}
    >
      {children}
    </h4>
  );
}

function InfoGrid({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "4px 12px" }}>
      {children}
    </div>
  );
}

function InfoItem({
  label,
  value,
  highlight,
  hint,
}: {
  label: string;
  value: string | React.ReactNode;
  highlight?: "erro" | "warn";
  hint?: string;
}) {
  return (
    <div style={{ fontSize: 12 }} title={hint}>
      <span style={{ color: COLORS.textMuted }}>{label}: </span>
      <span
        style={{
          fontWeight: 500,
          color: highlight === "erro" ? COLORS.danger : highlight === "warn" ? COLORS.warning : COLORS.text,
        }}
      >
        {value}
      </span>
    </div>
  );
}

/** Renderiza o valor da placa — com badge informativo quando vazia. */
function PlacaCell({ placa }: { placa: string | null }) {
  if (placa) return <>{placa}</>;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        fontSize: 11,
        fontWeight: 500,
        padding: "2px 8px",
        borderRadius: 4,
        background: COLORS.warningBg,
        color: COLORS.warningFg,
        border: `1px solid rgba(245,158,11,0.3)`,
      }}
    >
      <span aria-hidden>⚠</span>
      Sem cadastro no Club
    </span>
  );
}

function ValueCard({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div
      style={{
        background: highlight ? COLORS.errorBg : COLORS.bgWhite,
        border: `1px solid ${highlight ? `rgba(239,68,68,0.4)` : COLORS.borderLight}`,
        borderRadius: RADIUS.sm,
        padding: "8px 14px",
        minWidth: 110,
        textAlign: "center",
      }}
    >
      <div style={{ fontSize: 10, color: COLORS.textMuted, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 15, fontWeight: 600, color: highlight ? COLORS.danger : COLORS.text }}>{value}</div>
    </div>
  );
}

function LinkBadge({ href, label, color }: { href: string; label: string; color: string }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: "4px 10px",
        background: color + "15",
        color,
        borderRadius: 4,
        fontSize: 11,
        fontWeight: 500,
        textDecoration: "none",
        border: `1px solid ${color}40`,
      }}
    >
      {label}
      <svg
        width="10"
        height="10"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2.4"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden
      >
        <path d="M7 17L17 7M17 7H8M17 7v9" />
      </svg>
    </a>
  );
}

function StatusBadge({ label, value, type }: { label: string; value: string; type: "ok" | "warn" | "error" }) {
  const colors = {
    ok: { bg: COLORS.successBg, fg: COLORS.successFg },
    warn: { bg: COLORS.warningBg, fg: COLORS.warningFg },
    error: { bg: COLORS.errorBg, fg: COLORS.errorFg },
  };
  const c = colors[type];
  return (
    <div
      style={{
        padding: "6px 10px",
        background: c.bg,
        color: c.fg,
        borderRadius: RADIUS.sm,
        fontSize: 11,
        fontWeight: 600,
      }}
    >
      <div style={{ fontSize: 9, textTransform: "uppercase", opacity: 0.7, marginBottom: 2 }}>{label}</div>
      {value}
    </div>
  );
}

function ReincidenciaBadge({ div: d }: { div: DivergenciaCompleta }) {
  const dados = d.dados;
  if (dados.tem_devolucao_peca) {
    return (
      <span style={{ color: COLORS.successFg, fontWeight: 600 }}>
        Com devolucao
      </span>
    );
  }
  if (dados.tem_devolucao_outra_peca) {
    return (
      <span style={{ color: COLORS.warningFg, fontWeight: 600 }}>
        Dev. outra peca
      </span>
    );
  }
  if (dados.mesmo_fornecedor) {
    return (
      <span style={{ color: COLORS.errorFg, fontWeight: 600 }}>
        SEM devolucao (mesmo forn.)
      </span>
    );
  }
  return (
    <span style={{ color: COLORS.warningFg, fontWeight: 600 }}>
      SEM devolucao (outro forn.)
    </span>
  );
}

function ReincidenciaCard({ div: d }: { div: DivergenciaCompleta }) {
  const dados = d.dados;
  const sev = SEV_COLORS[d.severidade] ?? SEV_COLORS.info;

  // Determine left border color based on status
  const leftBorderColor = dados.tem_devolucao_peca
    ? "#17a34a" // green - has return
    : dados.tem_devolucao_outra_peca
      ? "#ea580c" // orange - has return for another part
      : "#dc2626"; // red - no return

  return (
    <div
      style={{
        background: COLORS.bgWhite,
        border: `1px solid ${COLORS.borderLight}`,
        borderLeft: `4px solid ${leftBorderColor}`,
        borderRadius: RADIUS.sm,
        padding: 14,
        marginBottom: 10,
        fontSize: 12,
        boxShadow: "0 1px 3px rgba(0,0,0,.05)",
      }}
    >
      {/* Cabecalho */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
        <span>{sev.icon}</span>
        <strong style={{ color: sev.fg }}>{d.titulo}</strong>
      </div>

      {/* Detalhes da peca */}
      <div style={{ color: COLORS.text, lineHeight: 1.6 }}>
        <div><strong>Peca:</strong> {dados.descricao_peca ?? "--"}</div>
        <div><strong>OC Anterior:</strong> {dados.oc_anterior ?? "--"} ({dados.data_anterior ?? "--"})</div>
        <div><strong>Fornecedor Anterior:</strong> {dados.fornecedor_anterior_nome ?? "--"}</div>
        <div>
          <strong>Mesmo Fornecedor:</strong>{" "}
          {dados.mesmo_fornecedor ? "Sim" : "Nao"}
        </div>
        <div>
          <strong>Devolucao:</strong>{" "}
          {dados.tem_devolucao_peca
            ? "Sim (desta peca)"
            : dados.tem_devolucao_outra_peca
              ? "Sim (outra peca)"
              : "Nenhuma devolucao encontrada"}
        </div>
        {dados.peca_descricao_devolucao && (
          <div><strong>Peca na Devolucao:</strong> {dados.peca_descricao_devolucao}</div>
        )}
        <div><strong>Total Reincidencias:</strong> {dados.qtd_reincidencias ?? 1}</div>
      </div>

      {/* Links */}
      <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
        {dados.link_oc_anterior && (
          <LinkBadge href={dados.link_oc_anterior} label="OC Anterior" color={COLORS.primary} />
        )}
        {dados.link_devolucao && (
          <LinkBadge href={dados.link_devolucao} label="Card Devolucao" color={COLORS.success} />
        )}
      </div>

      {/* Decisao para o analista */}
      <div
        style={{
          marginTop: 8,
          padding: "6px 10px",
          background: COLORS.bgWhite,
          borderRadius: 4,
          fontSize: 11,
          color: COLORS.text,
          borderLeft: `3px solid ${sev.fg}`,
        }}
      >
        <strong>Acao do analista:</strong>{" "}
        {dados.tem_devolucao_peca
          ? "APROVAR — devolucao confirmada para esta peca"
          : dados.sem_devolucao
            ? "RECUSAR — nao ha card de devolucao. Sinalizar ao comprador."
            : "VERIFICAR — ha devolucao, mas de outra peca. Confirmar com comprador."}
      </div>
    </div>
  );
}

function DivergenciaCard({ div: d }: { div: DivergenciaCompleta }) {
  const sev = SEV_COLORS[d.severidade] ?? SEV_COLORS.info;
  return (
    <div
      style={{
        background: COLORS.bgWhite,
        border: `1px solid ${COLORS.borderLight}`,
        borderLeft: `4px solid ${sev.fg}`,
        borderRadius: RADIUS.sm,
        padding: "10px 14px",
        marginBottom: 8,
        fontSize: 12,
        boxShadow: "0 1px 3px rgba(0,0,0,.05)",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 2 }}>
        <span>{sev.icon}</span>
        <strong style={{ color: sev.fg }}>[{d.regra}] {d.titulo}</strong>
      </div>
      <div style={{ color: COLORS.text, lineHeight: 1.5 }}>
        {d.descricao}
      </div>
    </div>
  );
}

const thSmall: React.CSSProperties = {
  padding: "6px 8px",
  textAlign: "left",
  fontSize: 10,
  textTransform: "uppercase",
  fontWeight: 600,
  color: COLORS.textSecondary,
};

// ============================================================
// Tabela Principal
// ============================================================

export function ResultadosTable({ resultados, ciliaMode, ciliaBaseUrl }: Props) {
  const [sortCol, setSortCol] = useState<SortCol | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [page, setPage] = useState(0);
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());

  useEffect(() => {
    setPage(0);
    setExpandedIds(new Set());
  }, [resultados]);

  function toggleExpand(id: number) {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }

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
        case "reincidencia": va = a.reincidencia; vb = b.reincidencia; break;
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
      <div style={{ ...cardPanel, padding: 48, textAlign: "center" }}>
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

  // Contagem de reincidencias para banner
  const totalReincidencias = resultados.filter(
    (r) => r.reincidencia && r.reincidencia !== "\u2014" && r.reincidencia !== "--"
  ).length;
  const reincSemDevolucao = resultados.filter(
    (r) => r.reincidencia && r.reincidencia.includes("sem_devolucao")
  ).length;

  const columns: { label: string; col: SortCol | null; align?: "right" }[] = [
    { label: "", col: null },
    { label: "N. OC", col: "id_pedido" },
    { label: "Placa", col: "placa" },
    { label: "Fornecedor", col: "fornecedor" },
    { label: "Valor Club", col: "valor_club", align: "right" },
    { label: "Valor PDF", col: "valor_pdf", align: "right" },
    { label: "Cot.", col: "qtd_cotacoes", align: "right" },
    { label: "Status", col: "status" },
    { label: "Reincidencia", col: "reincidencia" },
    { label: "Card", col: null },
    { label: "Motivo", col: null },
  ];

  return (
    <div>
      {/* Banner de alertas de reincidencia */}
      {totalReincidencias > 0 && (
        <div
          style={{
            padding: "10px 16px",
            marginBottom: 12,
            borderRadius: RADIUS.sm,
            background: reincSemDevolucao > 0 ? COLORS.errorBg : COLORS.warningBg,
            border: `1px solid ${reincSemDevolucao > 0 ? `rgba(239,68,68,0.4)` : `rgba(245,158,11,0.4)`}`,
            display: "flex",
            alignItems: "center",
            gap: 8,
            fontSize: 13,
          }}
        >
          <span style={{ fontSize: 18 }}>{reincSemDevolucao > 0 ? "\u26d4" : "\u26a0\ufe0f"}</span>
          <div>
            <strong>{totalReincidencias} OC(s) com reincidencia detectada</strong>
            {reincSemDevolucao > 0 && (
              <span style={{ color: COLORS.danger, marginLeft: 8 }}>
                ({reincSemDevolucao} SEM devolucao — devem ser RECUSADAS)
              </span>
            )}
            <div style={{ fontSize: 11, color: COLORS.textSecondary, marginTop: 2 }}>
              Clique na linha para expandir detalhes de cada OC. Verifique pecas, cards de devolucao e OCs anteriores.
            </div>
          </div>
        </div>
      )}

      <div style={{ ...cardPanel, overflow: "hidden" }}>
        <div style={{ overflowX: "auto", WebkitOverflowScrolling: "touch" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 900 }}>
            <thead>
              <tr style={{ background: COLORS.bgHover, color: COLORS.text }}>
                {columns.map((c) => (
                  <th
                    key={c.label || "expand"}
                    scope="col"
                    style={{
                      ...thStyle,
                      textAlign: c.align || "left",
                      cursor: c.col ? "pointer" : "default",
                      userSelect: "none",
                      whiteSpace: "nowrap",
                      ...(c.label === "" ? { width: 36 } : {}),
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
                const isExpanded = expandedIds.has(r.id);
                const hasReincidencia = r.reincidencia && r.reincidencia !== "\u2014";
                const reincSemDev = r.reincidencia?.includes("sem_devolucao");

                let motivo = (r.regras_falhadas ?? [])
                  .map((d) => `[${d.regra}] ${d.titulo}`)
                  .join("; ");
                if (r.status === "aguardando_ml") {
                  motivo = "Fornecedor ML -- manual";
                } else if (r.status === "ja_processada") {
                  motivo = `Ja em "${r.fase_pipefy_atual ?? "?"}"`;
                } else if (r.status === "sem_card_pipefy") {
                  motivo = "OC sem card no Pipefy";
                }

                // Reincidencia label — mapeamento exato alinhado com HTML/Excel
                let reincLabel = "\u2014";
                let reincColor: string = COLORS.textMuted;
                if (hasReincidencia) {
                  const ri = r.reincidencia!;
                  if (ri === "sim_sem_devolucao") {
                    reincLabel = "SEM devolucao";
                    reincColor = COLORS.errorFg;
                  } else if (ri === "sim_sem_devolucao_mesmo_forn") {
                    reincLabel = "SEM devolucao (mesmo forn.)";
                    reincColor = COLORS.errorFg;
                  } else if (ri === "sim_com_devolucao_peca" || ri === "sim_devolucao") {
                    reincLabel = "Devolucao da peca";
                    reincColor = COLORS.successFg;
                  } else if (ri === "sim_devolucao_outra_peca") {
                    reincLabel = "Dev. outra peca";
                    reincColor = COLORS.warningFg;
                  } else if (ri === "sim_mesmo_forn") {
                    reincLabel = "Mesmo fornecedor";
                    reincColor = COLORS.warningFg;
                  } else if (ri === "sim_outro_forn") {
                    reincLabel = "Outro fornecedor";
                    reincColor = COLORS.warningFg;
                  } else {
                    reincLabel = ri;
                    reincColor = COLORS.warningFg;
                  }
                }

                return (
                  <Fragment key={r.id}>
                    <tr
                      onClick={() => toggleExpand(r.id)}
                      role="button"
                      tabIndex={0}
                      aria-expanded={isExpanded}
                      aria-label={`OC ${r.id_pedido} — ${r.placa ?? "sem placa"} — clique para ${isExpanded ? "recolher" : "expandir"} detalhes`}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          toggleExpand(r.id);
                        }
                      }}
                      style={{
                        borderTop: `1px solid ${COLORS.borderRow}`,
                        cursor: "pointer",
                        background: isExpanded
                          ? COLORS.primaryDim
                          : hasReincidencia && reincSemDev
                            ? COLORS.errorBg
                            : paged.indexOf(r) % 2 === 1
                              ? COLORS.bg
                              : COLORS.bgWhite,
                        transition: "background 100ms ease",
                      }}
                      onMouseEnter={(e) => {
                        if (!isExpanded) e.currentTarget.style.background = COLORS.bgHover;
                      }}
                      onMouseLeave={(e) => {
                        if (!isExpanded) {
                          e.currentTarget.style.background =
                            hasReincidencia && reincSemDev
                              ? COLORS.errorBg
                              : paged.indexOf(r) % 2 === 1
                                ? COLORS.bg
                                : COLORS.bgWhite;
                        }
                      }}
                    >
                      {/* Expand arrow */}
                      <td style={{ ...tdStyle, textAlign: "center", width: 36, padding: "10px 6px" }}>
                        <span
                          style={{
                            display: "inline-block",
                            transform: isExpanded ? "rotate(90deg)" : "rotate(0deg)",
                            transition: "transform 150ms ease",
                            fontSize: 12,
                            color: COLORS.textSecondary,
                          }}
                        >
                          \u25b6
                        </span>
                      </td>
                      <td style={tdStyle}>{r.id_pedido}</td>
                      <td
                        style={{ ...tdStyle, fontWeight: 500 }}
                        title={
                          !r.placa
                            ? "Esta OC foi gerada no Club sem placa associada. Verificar manualmente."
                            : undefined
                        }
                      >
                        <PlacaCell placa={r.placa} />
                      </td>
                      <td style={{ ...tdStyle, maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {(r.fornecedor ?? "").slice(0, 25)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{fmtMoney(r.valor_club)}</td>
                      <td style={{
                        ...tdStyle,
                        textAlign: "right",
                        ...(r.valor_club != null && r.valor_pdf != null && r.valor_club !== r.valor_pdf
                          ? { color: COLORS.danger, fontWeight: 700 }
                          : {}),
                      }}>
                        {fmtMoney(r.valor_pdf)}
                        {r.valor_club != null && r.valor_pdf != null && r.valor_club !== r.valor_pdf && (
                          <div style={{ fontSize: 10, color: COLORS.danger, fontWeight: 400 }}>
                            {"\u2260"} Club: {fmtMoney(r.valor_club)}
                          </div>
                        )}
                      </td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>
                        <span style={{ color: r.qtd_cotacoes != null && r.qtd_cotacoes < 3 ? COLORS.danger : COLORS.text, fontWeight: r.qtd_cotacoes != null && r.qtd_cotacoes < 3 ? 700 : 400 }}>
                          {r.qtd_cotacoes ?? 0}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        <span
                          style={{
                            background: st.bg,
                            color: st.fg,
                            padding: "4px 10px",
                            borderRadius: 4,
                            fontSize: 12,
                            fontWeight: 600,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {st.text}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        {hasReincidencia ? (
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: 4,
                              background: reincSemDev ? COLORS.errorBg : COLORS.warningBg,
                              color: reincColor,
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 12,
                              fontWeight: 700,
                              whiteSpace: "nowrap",
                              minWidth: 110,
                              justifyContent: "center",
                              borderLeft: `3px solid ${reincSemDev ? COLORS.danger : COLORS.warning}`,
                            }}
                          >
                            {reincLabel}
                            {(() => {
                              const reincCount = (r.divergencias_json ?? []).filter((d) => d.regra === "R2").length;
                              return reincCount > 1 ? ` (${reincCount})` : "";
                            })()}
                          </span>
                        ) : (
                          <span style={{ color: COLORS.textMuted, fontSize: 12 }}>{"\u2014"}</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        {r.card_pipefy_link ? (
                          <a
                            href={r.card_pipefy_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={(e) => e.stopPropagation()}
                            style={{ color: COLORS.primary, fontSize: 11, textDecoration: "none" }}
                          >
                            Abrir
                          </a>
                        ) : (
                          <span style={{
                            color: "#e9a3ff",
                            fontSize: 10,
                            fontWeight: 700,
                            background: "rgba(217, 70, 239, 0.12)",
                            padding: "2px 6px",
                            borderRadius: 3,
                          }}>
                            SEM CARD
                          </span>
                        )}
                      </td>
                      <td style={{ ...tdStyle, color: COLORS.errorFg, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {motivo || "--"}
                      </td>
                    </tr>
                    {/* Detalhe expandido */}
                    {isExpanded && (
                      <tr key={`${r.id}-detail`}>
                        <td colSpan={columns.length} style={{ padding: 0 }}>
                          <DetalheOC r={r} ciliaMode={ciliaMode} ciliaBaseUrl={ciliaBaseUrl} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
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
