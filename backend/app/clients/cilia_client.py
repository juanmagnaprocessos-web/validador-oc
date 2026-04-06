"""Cliente Cilia — interface + stub.

⚠️ AS CREDENCIAIS E A DOCUMENTAÇÃO DA API CILIA AINDA NÃO CHEGARAM.

Este módulo expõe uma interface abstrata (`CiliaClient`) com uma implementação
stub (`CiliaStub`) que devolve dados realistas baseados num pequeno fixture
determinístico. Quando as credenciais chegarem, criar `CiliaHTTPClient` que
implementa a mesma interface — nenhum outro módulo precisa ser alterado.

Troca via variável de ambiente: `CILIA_MODE=stub` (default) ou `CILIA_MODE=http`.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from datetime import date, timedelta
from decimal import Decimal

from app.config import settings
from app.logging_setup import get_logger
from app.models import ItemCilia, OrcamentoCilia

logger = get_logger(__name__)


class CiliaClient(ABC):
    """Interface que qualquer implementação do Cilia deve respeitar."""

    @abstractmethod
    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        """Retorna o orçamento mais recente do Cilia para a placa dada."""

    async def close(self) -> None:
        pass


# ----------------------------------------------------------------------
# Stub — gera dados determinísticos por placa para desenvolvimento
# ----------------------------------------------------------------------

class CiliaStub(CiliaClient):
    """Implementação fake: gera orçamento determinístico por placa.

    Usado enquanto as credenciais reais do Cilia não estão disponíveis.
    A mesma placa sempre retorna os mesmos valores (hash determinístico),
    o que permite escrever testes reproduzíveis contra o stub.
    """

    def __init__(self) -> None:
        self._cache: dict[str, OrcamentoCilia] = {}

    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        placa_norm = placa.replace("-", "").upper().strip()
        if not placa_norm:
            return None

        if placa_norm in self._cache:
            return self._cache[placa_norm]

        # Placas terminadas em "99" → simular "não encontrado"
        if placa_norm.endswith("99"):
            logger.debug("Cilia stub: placa %s não encontrada (simulado)", placa_norm)
            return None

        seed = int(hashlib.md5(placa_norm.encode()).hexdigest(), 16)

        # Valor base entre R$ 500 e R$ 5000 (determinístico por placa)
        valor_total = Decimal(f"{500 + (seed % 4500)}.{seed % 100:02d}")
        qtd_itens = 1 + (seed % 4)  # 1 a 4 itens

        itens = []
        descricoes = [
            "Para-choque dianteiro",
            "Farol esquerdo",
            "Capô",
            "Paralama dianteiro direito",
            "Retrovisor lateral",
            "Porta dianteira",
            "Para-lama traseiro",
            "Tampa traseira",
        ]
        valor_restante = valor_total
        for i in range(qtd_itens):
            if i == qtd_itens - 1:
                vi = valor_restante
            else:
                vi = (valor_total / qtd_itens).quantize(Decimal("0.01"))
                valor_restante -= vi
            itens.append(
                ItemCilia(
                    descricao=descricoes[(seed + i) % len(descricoes)],
                    quantidade=1,
                    valor_unitario=vi,
                    valor_total=vi,
                )
            )

        orc = OrcamentoCilia(
            placa=placa_norm,
            numero_orcamento=f"CIL-{seed % 100000:05d}",
            data=date.today() - timedelta(days=1),
            valor_total=valor_total,
            itens=itens,
            encontrado=True,
        )
        self._cache[placa_norm] = orc
        return orc


# ----------------------------------------------------------------------
# Placeholder HTTP — a preencher quando chegarem as credenciais
# ----------------------------------------------------------------------

class CiliaHTTPClient(CiliaClient):
    """Implementação HTTP real — TODO quando as credenciais do Cilia chegarem.

    Assim que tivermos:
      - URL base da API
      - Método de autenticação (presumivelmente usuário/senha → cookie/token)
      - Schema da resposta de orçamentos
    basta preencher os métodos abaixo. O resto do sistema não precisa mudar.
    """

    def __init__(
        self,
        api_url: str | None = None,
        login: str | None = None,
        senha: str | None = None,
    ) -> None:
        self._api_url = api_url or settings.cilia_api_url
        self._login = login or settings.cilia_login
        self._senha = senha or settings.cilia_senha
        if not self._api_url:
            raise RuntimeError(
                "CILIA_API_URL não configurado — use CILIA_MODE=stub até "
                "receber as credenciais"
            )
        # TODO: httpx.AsyncClient + autenticação

    async def consultar_por_placa(self, placa: str) -> OrcamentoCilia | None:
        raise NotImplementedError(
            "CiliaHTTPClient ainda não implementado — use CILIA_MODE=stub"
        )


# ----------------------------------------------------------------------
# Factory
# ----------------------------------------------------------------------

def build_cilia_client() -> CiliaClient:
    """Escolhe a implementação conforme settings.cilia_mode."""
    mode = (settings.cilia_mode or "stub").lower()
    if mode == "stub":
        logger.info("Cilia: usando CiliaStub (modo desenvolvimento)")
        return CiliaStub()
    if mode == "http":
        logger.info("Cilia: usando CiliaHTTPClient")
        return CiliaHTTPClient()
    raise RuntimeError(f"CILIA_MODE inválido: {mode!r} (use 'stub' ou 'http')")
