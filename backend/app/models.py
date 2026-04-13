"""Modelos de domínio (Pydantic) compartilhados entre clients, validators e services."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ======================================================================
# Enums
# ======================================================================

class StatusValidacao(str, Enum):
    APROVADA = "aprovada"
    DIVERGENCIA = "divergencia"
    BLOQUEADA = "bloqueada"
    AGUARDANDO_ML = "aguardando_ml"      # Fornecedor Mercado Livre — requer validação manual
    JA_PROCESSADA = "ja_processada"      # Card já estava fora da fase "Validação" no Pipefy
    SEM_CARD_PIPEFY = "sem_card_pipefy"  # OC do Club sem card correspondente no Pipefy


class Severidade(str, Enum):
    ERRO = "erro"        # bloqueia envio ao financeiro
    ALERTA = "alerta"    # requer revisão do analista
    INFO = "info"        # apenas registra


class FasePipefy(str, Enum):
    VALIDACAO = "Validação Ordem de Compra"
    AGUARDAR_PECAS = "Aguardar Peças"
    PROGRAMAR_PAGAMENTO = "Programar Pagamento"
    COMPRAS_ML = "Compras Mercado Livre"
    INFORMACOES_INCORRETAS = "Informações Incorretas"


# ======================================================================
# Dados do Club da Cotação
# ======================================================================

class Fornecedor(BaseModel):
    model_config = ConfigDict(extra="allow")
    for_id: str | None = None
    for_nome: str | None = None
    for_status: str | None = None       # "1" = ativo
    for_excluido: str | None = None     # "0" = não excluído
    for_cnpj: str | None = None


class ItemOC(BaseModel):
    model_config = ConfigDict(extra="allow")
    product_id: str | None = None
    descricao: str | None = None
    quantity: int | float = 0
    unit_price: Decimal | None = None
    total_price: Decimal | None = None


class ProdutoCotacao(BaseModel):
    """Item da aba Produtos da cotação — usado para R2 (duplicidade)."""
    model_config = ConfigDict(extra="allow")
    produto_id: str | None = None
    descricao: str | None = None
    quantidade: float = 0
    ean: str | None = None
    cod_interno: str | None = None
    valor_unitario: Decimal | None = None   # unit_price do Club
    valor_total: Decimal | None = None      # total_price do Club


class Concorrente(BaseModel):
    model_config = ConfigDict(extra="allow")
    id_fornecedor: str | None = None
    fornecedor_nome: str | None = None


class OrdemCompra(BaseModel):
    """OC consolidada a partir do listarpedidos + detalhes.

    Nota importante: no payload do Club, `usu_nome` contém o **nome do
    fornecedor** (ex: "CURINGA", "AMORIM") — não o do comprador. Quem de
    fato criou a OC é identificado pelo `created_by` (ID numérico). Para
    resolver o ID em nome+email, usamos a tabela auxiliar `compradores`
    em SQLite.
    """
    model_config = ConfigDict(extra="allow")

    id_pedido: str
    id_cotacao: str | None = None
    identificador: str | None = None     # placa com hífen (ex: "PQX-2I72")
    valor_pedido: Decimal | None = None
    forma: str | None = None              # "A Vista", "Pix", "Faturado"...
    created_by: int | None = None         # ID do usuário Club que criou (real)
    usu_nome_club: str | None = None      # "usu_nome" do Club = fornecedor (legado, debug)
    status: str | None = None
    fornecedor: Fornecedor | None = None
    divergencia_flag: bool = Field(False, alias="divergencia")
    data_pedido: date | None = None
    items: list[ItemOC] = Field(default_factory=list)

    # preenchido a posteriori pelo orchestrator (via tabela compradores)
    comprador_nome: str | None = None
    comprador_email: str | None = None

    @property
    def placa_normalizada(self) -> str:
        if not self.identificador:
            return ""
        # Remove hifen E espacos para alinhar com PipefyClient._normalizar_placa.
        # Sem essa paridade, lookups em indice_cards_historicos dao miss em placas
        # vindas do Club com espaco, suprimindo alertas R2 cross-time.
        return self.identificador.replace("-", "").replace(" ", "").upper().strip()

    @property
    def eh_mercado_livre(self) -> bool:
        """True se o fornecedor é Mercado Livre (match por substring no nome)."""
        if not self.fornecedor or not self.fornecedor.for_nome:
            return False
        return "MERCADO LIVRE" in self.fornecedor.for_nome.upper()


# ======================================================================
# Dados do Cilia
# ======================================================================

class ItemCilia(BaseModel):
    descricao: str
    quantidade: float
    valor_unitario: Decimal | None = None
    valor_total: Decimal | None = None


class OrcamentoCilia(BaseModel):
    placa: str
    numero_orcamento: str | None = None
    data: date | None = None
    valor_total: Decimal | None = None
    itens: list[ItemCilia] = Field(default_factory=list)
    encontrado: bool = True


# ======================================================================
# Dados do Pipefy
# ======================================================================

class CardPipefy(BaseModel):
    id: str
    title: str                         # = placa sem hífen
    phase_id: str | None = None
    phase_name: str | None = None
    campos: dict[str, Any] = Field(default_factory=dict)
    codigo_oc: str | None = None        # campo "Código da OC" — match com OC.id_pedido do Club
    anexo_oc_url: str | None = None     # campo "Ordem de compra" — PDF
    anexo_cilia_url: str | None = None  # campo "Orçamento Cília" — PDF (descoberto)
    valor_card: Decimal | None = None   # campo "Valor" (currency) estruturado — REFERÊNCIA PRINCIPAL
    valor_extraido_pdf: Decimal | None = None
    descricao_pecas: str | None = None  # long_text "Descrição das Peças"
    created_at: datetime | None = None  # criado_em do card no Pipefy (para filtro D-1)
    # Campos do start form usados para decidir a fase de destino — são a
    # FONTE CANÔNICA da forma de pagamento e da origem da peça (não os
    # campos análogos do Club, que trazem prazo de pagamento, não forma).
    forma_pagamento: str | None = None  # "PIX" | "Cartão de Crédito" | "Faturado" | "Boleto"
    origem_peca: str | None = None      # "Mercado Livre / Site" | "Estoque" | "Auto Peça"

    @property
    def eh_mercado_livre(self) -> bool:
        """True se o card é Mercado Livre — preferimos o campo 'Origem da peça'
        do start form, que é a fonte oficial preenchida pelo comprador."""
        return (self.origem_peca or "").strip().lower().startswith("mercado livre")


# ======================================================================
# Resultado de validação
# ======================================================================

class Divergencia(BaseModel):
    regra: str                              # "R1", "R2", ...
    titulo: str                             # resumo curto
    descricao: str                          # explicação detalhada
    severidade: Severidade = Severidade.ERRO
    dados: dict[str, Any] = Field(default_factory=dict)


# ======================================================================
# Auth — perfis e usuários
# ======================================================================

class Perfil(BaseModel):
    id: int
    nome: str
    descricao: str | None = None
    permissoes: list[str] = Field(default_factory=list)
    criado_em: str


class PerfilCreate(BaseModel):
    nome: str = Field(..., min_length=2, max_length=50)
    descricao: str | None = None
    permissoes: list[str] = Field(default_factory=list)


class PerfilUpdate(BaseModel):
    nome: str | None = Field(None, min_length=2, max_length=50)
    descricao: str | None = None
    permissoes: list[str] | None = None


class Usuario(BaseModel):
    id: int
    username: str
    nome: str
    email: str | None = None
    perfil_id: int
    perfil_nome: str | None = None
    ativo: bool
    must_change_password: bool
    criado_em: str
    ultimo_login: str | None = None


class UsuarioCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    nome: str = Field(..., min_length=2, max_length=120)
    email: str | None = None
    perfil_id: int
    senha_temporaria: str = Field(..., min_length=8, max_length=128)


class UsuarioUpdate(BaseModel):
    nome: str | None = Field(None, min_length=2, max_length=120)
    email: str | None = None
    perfil_id: int | None = None
    ativo: bool | None = None


class TrocarSenhaRequest(BaseModel):
    senha_atual: str
    nova_senha: str = Field(..., min_length=8, max_length=128)


class ResetSenhaResponse(BaseModel):
    nova_senha_temporaria: str


class OcOrfa(BaseModel):
    """OC do Club que NÃO tem card correspondente no Pipefy.
    Listada separadamente no relatório para o analista identificar
    cards faltantes ou atrasados."""
    id_pedido: str
    id_cotacao: str | None = None
    identificador: str | None = None        # placa
    valor: Decimal | None = None
    fornecedor: str | None = None
    comprador: str | None = None
    forma_pagamento: str | None = None      # prazo do Club ("07 dias", "A Vista"...)
    data_pedido: date | None = None
    # Verificação de duplicidade interna de peças (R2 parte 1) aplicada
    # também às OCs órfãs — "Sim" / "Não" / "—" (sem cotação para checar).
    peca_duplicada: str = "—"
    qtd_produtos: int | None = None
    qtd_cotacoes: int | None = None  # concorrentes da cotação (R1)
    # Divergências da R2 cross-time (parte 2) — incluem dados estruturados
    # com link para a OC anterior e link para o card de devolução, se houver.
    divergencias: list[Divergencia] = Field(default_factory=list)
    # Resumo simples de "houve reincidência cross-time?", para a coluna do relatório.
    # Resumo simples de "houve reincidência cross-time?", para a coluna do relatório.
    # Valores possíveis: "—" | "sim_sem_devolucao" | "sim_sem_devolucao_mesmo_forn"
    # | "sim_devolucao_outra_peca" | "sim_mesmo_forn" | "sim_outro_forn"
    # | "sim_com_devolucao_peca"
    reincidencia: str = "—"
    # Cancelamento detectado no pipe principal (fases Inf. Incorretas / Cancelados)
    cancelamento: str = "—"   # "—" | "info_incorretas" | "cancelado" | "ambos"
    cancelamento_card_id: str | None = None
    # Lista de produtos efetivamente comprados nesta OC (vem do
    # `get_order_details(id_pedido).items` do Club).
    produtos: list[ProdutoCotacao] = Field(default_factory=list)
    # Chaves de produto (mesma chave da R2) que são reincidentes — pré
    # computado pelo orchestrator para o template marcar visualmente.
    chaves_reincidentes: list[str] = Field(default_factory=list)
    # Todas as duplicidades históricas da placa (90d), incluindo peças que
    # NÃO estão nesta OC. Cada item é um dict com chave_produto, descricao,
    # total_ocorrencias, ids_pedido, datas_oc, fornecedores, status_devolucao.
    duplicidades_placa: list[dict] = Field(default_factory=list)


class ContextoValidacao(BaseModel):
    """Tudo que foi coletado para uma OC antes de aplicar as regras."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    oc: OrdemCompra
    concorrentes: list[Concorrente] = Field(default_factory=list)
    produtos_cotacao: list[ProdutoCotacao] = Field(default_factory=list)
    orcamento_cilia: OrcamentoCilia | None = None
    card_pipefy: CardPipefy | None = None
    data_d1: date
    # Historico pre-carregado (Pipefy + Club) indexado por chave_produto.
    # Quando presente, a R2 cross-time pula a query no SQLite local
    # (`historico_produtos_oc`) e usa este dict — eliminando a dependencia
    # do backfill do Club que nao converge em ambientes com memoria/tempo
    # limitados (ex: Render Free). Formato: chave_produto -> lista de dicts
    # compativeis com o retorno de `carregar_historico_bulk`.
    historico_indexado: dict[str, list[dict[str, Any]]] | None = None


class ResultadoValidacao(BaseModel):
    oc: OrdemCompra
    status: StatusValidacao
    divergencias: list[Divergencia] = Field(default_factory=list)
    fase_destino: FasePipefy | None = None
    valor_card: Decimal | None = None     # campo "Valor" do card Pipefy — REFERÊNCIA PRINCIPAL
    valor_club: Decimal | None = None
    valor_pdf: Decimal | None = None      # extraído do anexo PDF — auditoria
    valor_cilia: Decimal | None = None
    qtd_cotacoes: int | None = None
    qtd_produtos: int | None = None
    peca_duplicada: str = "Não"           # "Sim" / "Não" / "Verificar"
    abatimento_fornecedor: str = "Não"
    card_pipefy_id: str | None = None
    fase_pipefy_atual: str | None = None   # nome da fase em que o card estava no Pipefy
    validado_em: datetime = Field(default_factory=datetime.now)
    # Resumo da R2 cross-time (parte 2) — populado pelo orchestrator a
    # partir das `divergencias` da R2 cross-time + cache de devoluções.
    # Valores possíveis: "—" | "sim_sem_devolucao" | "sim_sem_devolucao_mesmo_forn"
    # | "sim_devolucao_outra_peca" | "sim_mesmo_forn" | "sim_outro_forn"
    # | "sim_com_devolucao_peca"
    reincidencia: str = "—"
    # Cancelamento detectado no pipe principal (fases Inf. Incorretas / Cancelados)
    cancelamento: str = "—"        # "—" | "info_incorretas" | "cancelado" | "ambos"
    cancelamento_card_id: str | None = None
    # Subset das `divergencias` que vieram da R2 cross-time, para o template
    # renderizar o detalhe expansível com TODAS as peças reincidentes.
    divergencias_cross: list[Divergencia] = Field(default_factory=list)
    # Lista de produtos efetivamente comprados nesta OC (vem do
    # `get_order_details(id_pedido).items` do Club). Usado pelo template
    # para renderizar o expansor "Listar peças".
    produtos: list[ProdutoCotacao] = Field(default_factory=list)
    # Conjunto de chaves de produto (mesma chave da R2) que são
    # reincidentes nesta validação. Pré-computado pelo orchestrator
    # para evitar loop O(n*m) no Jinja2.
    chaves_reincidentes: list[str] = Field(default_factory=list)
    # Forma de pagamento "canônica" — vem do CARD do Pipefy quando
    # disponível, senão fallback para `oc.forma` (Club). É a fonte
    # oficial usada pelo `_decidir_fase()`.
    forma_pagamento_canonica: str | None = None
    # Todas as duplicidades históricas da placa (90d), incluindo peças que
    # NÃO estão nesta OC. Cada item é um dict com chave_produto, descricao,
    # total_ocorrencias, ids_pedido, datas_oc, fornecedores, status_devolucao.
    duplicidades_placa: list[dict] = Field(default_factory=list)

    @property
    def aprovada(self) -> bool:
        return self.status == StatusValidacao.APROVADA

    @property
    def requer_acao_pipefy(self) -> bool:
        """Falso apenas para JA_PROCESSADA (card já fora da fase Validação).
        ML AGORA é movido para Compra Mercado Livre automaticamente — antes
        ficava parado aguardando analista."""
        return self.status != StatusValidacao.JA_PROCESSADA

    @property
    def motivo_resumido(self) -> str:
        """Apenas divergências bloqueantes (ERRO/ALERTA). INFO é filtrado."""
        if not self.divergencias:
            return ""
        bloqueantes = [
            d for d in self.divergencias
            if d.severidade in (Severidade.ERRO, Severidade.ALERTA)
        ]
        return "; ".join(d.titulo for d in bloqueantes)
