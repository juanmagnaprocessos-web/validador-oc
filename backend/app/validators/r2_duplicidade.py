"""R2 — Quantidade sem duplicidade.

Parte 1 (intra-cotação): detecta peça repetida na MESMA cotação.
Parte 2 (cross-time):    detecta peça repetida em OCs anteriores da
                          MESMA placa (janela `R2_JANELA_DIAS`).
                          Se há devolução em aberto para a placa no
                          pipe Pipefy de Devolução, é re-compra legítima
                          (alerta leve). Senão, alerta forte.
- Mesma peça para fornecedores diferentes → alerta leve.
- Modo controlado por `R2_MODO`: alerta | bloqueio | off.
"""
from __future__ import annotations

from collections import defaultdict

from app.config import settings
from app.db import buscar_reincidencias, get_devolucoes_por_placa, get_devolucoes_por_oc
from app.models import ContextoValidacao, Divergencia, Severidade
from app.utils.chave_produto import chave_produto_de_obj
from app.utils.historico_bulk import carregar_historico_bulk
from app.utils.normalizacao_pecas import descricoes_similares, THRESHOLD_MATCH
from app.validators.base import Regra


def _chave_produto(p) -> str:
    """Normaliza chave do produto para detectar duplicidade.

    Delega para a função canônica em utils.chave_produto para garantir
    consistência com historico_produtos e orchestrator.
    """
    return chave_produto_de_obj(p)


def _link_card_pipefy(card_id: str | None) -> str | None:
    """URL absoluta para o card no Pipefy principal (Sinistro Logística)."""
    if not card_id:
        return None
    return f"https://app.pipefy.com/pipes/{settings.pipe_id}#cards/{card_id}"


def _link_card_devolucao(card_id: str) -> str:
    """URL absoluta para o card no pipe de Devolução de Peças."""
    return (
        f"https://app.pipefy.com/pipes/{settings.pipefy_pipe_devolucao_id}"
        f"#cards/{card_id}"
    )


def detectar_reincidencias(
    *,
    placa_normalizada: str,
    identificador: str | None,
    id_pedido_atual: str,
    fornecedor_id: str | None,
    produtos,
    data_d1,
    _historico_bulk: dict[str, list] | None = None,
    _historico_pipefy_items: dict[str, list[dict]] | None = None,
) -> list[Divergencia]:
    """Função utilitária stateless: dado um conjunto mínimo de dados de
    uma OC + sua lista de produtos, retorna a lista de Divergencias
    cross-time (R2 parte 2).

    É usada tanto pela R2Duplicidade.validar (cards no Pipefy) quanto
    pelo orchestrator no loop das OCs órfãs (que não passam pelas regras).

    A severidade segue exatamente a mesma hierarquia:
      - off: nenhuma divergência
      - alerta: tudo INFO (aparece no relatório, NÃO bloqueia)
      - bloqueio: ALERTA leve / ALERTA forte / ERRO (bloqueia + move)

    Fonte do histórico (em ordem de prioridade):
      1. `_historico_pipefy_items` — pré-carregado via Pipefy+Club (novo
         fluxo, default em produção). Já vem filtrado pela placa e com a
         OC atual excluída; apenas filtramos por chave_produto aqui.
      2. `_historico_bulk` — pré-carregado via SQLite local (legado).
      3. Query SQL ad-hoc (fallback).
    """
    if settings.r2_modo == "off" or not placa_normalizada:
        return []

    modo_bloqueio = settings.r2_modo == "bloqueio"
    sev_default = Severidade.ALERTA if modo_bloqueio else Severidade.INFO
    sev_suspeito = Severidade.ERRO if modo_bloqueio else Severidade.INFO

    # Prioridade 1: histórico via Pipefy (novo fluxo)
    if _historico_pipefy_items is not None:
        _historico_bulk = _historico_pipefy_items
    # Prioridade 2/3: histórico via SQLite (legado / fallback)
    elif _historico_bulk is None:
        _historico_bulk = carregar_historico_bulk(
            placa_normalizada,
            data_max=data_d1,
            dias=settings.r2_janela_dias,
            ignorar_id_pedido=id_pedido_atual,
        )

    # Pré-carrega devoluções por placa como fallback (compatibilidade)
    devolucoes_placa = get_devolucoes_por_placa(placa_normalizada)

    out: list[Divergencia] = []
    ja_reportadas: set[str] = set()

    for p in produtos:
        chave = _chave_produto(p)
        if chave in ja_reportadas:
            continue
        # Lookup O(1) no dict pré-carregado em vez de query SQL
        reincidencias = _historico_bulk.get(chave, [])
        if not reincidencias:
            continue
        ja_reportadas.add(chave)

        ant = reincidencias[0]
        mesmo_fornecedor = bool(
            fornecedor_id
            and ant.get("fornecedor_id")
            and str(fornecedor_id) == str(ant.get("fornecedor_id"))
        )

        # --- Correlação de devolução por peça específica ---
        # 1) Buscar devolução pelo n_oc da OC ANTERIOR (mais preciso)
        oc_anterior_id = ant.get("id_pedido") or ""
        devs_por_oc = get_devolucoes_por_oc(oc_anterior_id)

        descricao_peca = getattr(p, "descricao", None) or chave
        dev_match = None           # card de devolução que casa com esta peça
        tem_dev_peca = False       # devolução confirmada para ESTA peça
        tem_dev_outra_peca = False  # há devolução da OC anterior, mas de outra peça

        if devs_por_oc:
            # Verificar se alguma devolução da OC anterior é da mesma peça
            for d in devs_por_oc:
                peca_dev = d.get("peca_descricao") or ""
                score = descricoes_similares(descricao_peca, peca_dev)
                if score >= THRESHOLD_MATCH:
                    dev_match = d
                    tem_dev_peca = True
                    break
            if not tem_dev_peca:
                tem_dev_outra_peca = True
                # Fallback: pegar o primeiro card da OC como referência
                dev_match = devs_por_oc[0]

        # 2) Fallback: buscar por placa (caso n_oc não esteja preenchido)
        if not dev_match and devolucoes_placa:
            for d in devolucoes_placa:
                peca_dev = d.get("peca_descricao") or ""
                score = descricoes_similares(descricao_peca, peca_dev)
                if score >= THRESHOLD_MATCH:
                    dev_match = d
                    tem_dev_peca = True
                    break
            if not dev_match:
                # Há devolução da placa mas não casa com esta peça
                tem_dev_outra_peca = True

        tem_devolucao = tem_dev_peca  # só conta se for da peça específica

        link_dev = (
            _link_card_devolucao(dev_match["card_id"])
            if dev_match
            else None
        )

        # --- Determinar severidade e rótulo ---
        if tem_dev_peca:
            severidade = sev_default
            rotulo = "reincidência com devolução da peça"
            acao = "Re-compra legítima (devolução aberta para esta peça)"
        elif tem_dev_outra_peca:
            severidade = sev_suspeito
            rotulo = "reincidência — devolução de OUTRA peça"
            acao = "ALERTA: há devolução da OC anterior, mas de outra peça"
        elif mesmo_fornecedor:
            severidade = sev_suspeito
            rotulo = "reincidência SEM devolução (mesmo fornecedor)"
            acao = "POSSÍVEL COMPRA DUPLICADA — sem devolução aberta"
        else:
            severidade = sev_default
            rotulo = "reincidência SEM devolução (outro fornecedor)"
            acao = "Mesma peça em fornecedor diferente — sem devolução"

        descricao = (
            f"{acao}. Peça '{descricao_peca}' já comprada para a placa "
            f"{identificador or placa_normalizada} em "
            f"{ant.get('data_oc')} (OC {ant.get('id_pedido')}, "
            f"fornecedor {ant.get('fornecedor_nome') or '—'})."
        )
        if dev_match:
            descricao += (
                f" Devolução: card {dev_match.get('card_id')} "
                f"(fase '{dev_match.get('fase_atual')}', "
                f"peça '{dev_match.get('peca_descricao') or '—'}', "
                f"OC origem {dev_match.get('n_oc') or '—'})."
            )
        else:
            descricao += " Nenhum card de devolução encontrado."

        link_anterior = _link_card_pipefy(ant.get("card_pipefy_id"))
        out.append(
            Divergencia(
                regra="R2",
                titulo=f"Peça duplicada (cross-time): {rotulo}",
                descricao=descricao,
                severidade=severidade,
                dados={
                    "placa": placa_normalizada,
                    "chave_produto": chave,
                    "descricao_peca": descricao_peca,
                    "oc_anterior": ant.get("id_pedido"),
                    "data_anterior": ant.get("data_oc"),
                    "fornecedor_anterior_id": ant.get("fornecedor_id"),
                    "fornecedor_anterior_nome": ant.get("fornecedor_nome"),
                    "mesmo_fornecedor": mesmo_fornecedor,
                    "tem_devolucao_peca": tem_dev_peca,
                    "tem_devolucao_outra_peca": tem_dev_outra_peca,
                    "sem_devolucao": not tem_dev_peca and not tem_dev_outra_peca,
                    "qtd_reincidencias": len(reincidencias),
                    "link_oc_anterior": link_anterior,
                    "link_devolucao": link_dev,
                    "card_devolucao_id": (
                        dev_match["card_id"] if dev_match else None
                    ),
                    "peca_descricao_devolucao": (
                        dev_match.get("peca_descricao") if dev_match else None
                    ),
                },
            )
        )

    return out


class R2Duplicidade(Regra):
    codigo = "R2"
    nome = "Quantidade sem duplicidade"

    def validar(self, contexto: ContextoValidacao) -> list[Divergencia]:
        out: list[Divergencia] = []
        produtos = contexto.produtos_cotacao

        # ---------- Parte 2: cross-time (reincidência) ----------
        # Roda ANTES da parte 1 para que o relatório priorize o sinal
        # cross-time (que é o que o usuário pediu como prioridade).
        out.extend(self._verificar_cross_time(contexto))

        # 1) Detectar duplicidade por chave
        grupos: dict[str, list] = defaultdict(list)
        for p in produtos:
            grupos[_chave_produto(p)].append(p)

        duplicados = {k: v for k, v in grupos.items() if len(v) > 1}
        if duplicados:
            detalhes = []
            for k, itens in duplicados.items():
                qtd_total = sum(float(getattr(i, "quantidade", 0) or 0) for i in itens)
                detalhes.append(
                    f"{k} ({len(itens)}× = qtd total {qtd_total})"
                )
            out.append(
                Divergencia(
                    regra=self.codigo,
                    titulo=f"Peça duplicada: {len(duplicados)} item(ns)",
                    descricao=(
                        "Peças aparecem mais de uma vez na cotação: "
                        + "; ".join(detalhes)
                        + ". Verificar se é para fornecedores diferentes "
                          "(pode ser legítimo) e se há devolução aberta no Pipefy."
                    ),
                    severidade=Severidade.ALERTA,
                    dados={"duplicados": list(duplicados.keys())},
                )
            )

        # 2) Comparar quantidade total Club vs Cilia
        if contexto.orcamento_cilia and contexto.orcamento_cilia.encontrado:
            qtd_club = sum(
                float(getattr(p, "quantidade", 0) or 0) for p in produtos
            )
            qtd_cilia = sum(
                float(i.quantidade or 0) for i in contexto.orcamento_cilia.itens
            )
            if qtd_club and qtd_cilia and abs(qtd_club - qtd_cilia) > 0.01:
                # Cilia em stub → INFO (não bloqueia); em http → ERRO
                sev = (
                    Severidade.ERRO
                    if settings.cilia_mode == "http"
                    else Severidade.INFO
                )
                out.append(
                    Divergencia(
                        regra=self.codigo,
                        titulo=(
                            f"Qtd Club ({qtd_club:g}) ≠ Cilia ({qtd_cilia:g})"
                        ),
                        descricao=(
                            f"A soma de quantidades dos produtos da cotação "
                            f"({qtd_club:g}) não bate com o orçamento do Cilia "
                            f"({qtd_cilia:g}) para a placa "
                            f"{contexto.oc.placa_normalizada}."
                            + (
                                ""
                                if settings.cilia_mode == "http"
                                else " [Cilia em modo STUB — informativo]"
                            )
                        ),
                        severidade=sev,
                        dados={"qtd_club": qtd_club, "qtd_cilia": qtd_cilia},
                    )
                )

        return out

    # ------------------------------------------------------------------
    # Parte 2 — cross-time (reincidência cross-OC, janela R2_JANELA_DIAS)
    # Wrapper que delega para a função utilitária stateless
    # `detectar_reincidencias`, também usada pelo orchestrator nas órfãs.
    # ------------------------------------------------------------------
    def _verificar_cross_time(
        self, contexto: ContextoValidacao
    ) -> list[Divergencia]:
        oc = contexto.oc
        forn_id = oc.fornecedor.for_id if oc.fornecedor else None
        return detectar_reincidencias(
            placa_normalizada=oc.placa_normalizada,
            identificador=oc.identificador,
            id_pedido_atual=oc.id_pedido,
            fornecedor_id=str(forn_id) if forn_id else None,
            produtos=contexto.produtos_cotacao,
            data_d1=contexto.data_d1,
            _historico_pipefy_items=contexto.historico_indexado,
        )
