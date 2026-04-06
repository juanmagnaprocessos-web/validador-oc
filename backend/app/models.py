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
        return self.identificador.replace("-", "").upper().strip()

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
    codigo_oc: str | None = None        # campo "Código da OC"
    anexo_oc_url: str | None = None     # campo "Ordem de compra" — PDF
    anexo_cilia_url: str | None = None  # campo "Orçamento Cília" — PDF (descoberto)
    valor_card: Decimal | None = None   # campo "Valor" (currency) estruturado
    valor_extraido_pdf: Decimal | None = None
    descricao_pecas: str | None = None  # long_text "Descrição das Peças"


# ======================================================================
# Resultado de validação
# ======================================================================

class Divergencia(BaseModel):
    regra: str                              # "R1", "R2", ...
    titulo: str                             # resumo curto
    descricao: str                          # explicação detalhada
    severidade: Severidade = Severidade.ERRO
    dados: dict[str, Any] = Field(default_factory=dict)


class ContextoValidacao(BaseModel):
    """Tudo que foi coletado para uma OC antes de aplicar as regras."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    oc: OrdemCompra
    concorrentes: list[Concorrente] = Field(default_factory=list)
    produtos_cotacao: list[ProdutoCotacao] = Field(default_factory=list)
    orcamento_cilia: OrcamentoCilia | None = None
    card_pipefy: CardPipefy | None = None
    data_d1: date


class ResultadoValidacao(BaseModel):
    oc: OrdemCompra
    status: StatusValidacao
    divergencias: list[Divergencia] = Field(default_factory=list)
    fase_destino: FasePipefy | None = None
    valor_club: Decimal | None = None
    valor_pdf: Decimal | None = None
    valor_cilia: Decimal | None = None
    qtd_cotacoes: int | None = None
    qtd_produtos: int | None = None
    peca_duplicada: str = "Não"           # "Sim" / "Não" / "Verificar"
    abatimento_fornecedor: str = "Não"
    card_pipefy_id: str | None = None
    fase_pipefy_atual: str | None = None   # nome da fase em que o card estava no Pipefy
    validado_em: datetime = Field(default_factory=datetime.now)

    @property
    def aprovada(self) -> bool:
        return self.status == StatusValidacao.APROVADA

    @property
    def requer_acao_pipefy(self) -> bool:
        """Falso para ML (aguarda analista) e já processada (não tocar)."""
        return self.status not in (
            StatusValidacao.AGUARDANDO_ML,
            StatusValidacao.JA_PROCESSADA,
        )

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
