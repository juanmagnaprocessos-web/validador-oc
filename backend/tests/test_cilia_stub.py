import pytest

from app.clients.cilia_client import CiliaStub, build_cilia_client


@pytest.mark.asyncio
async def test_stub_retorna_determinístico():
    c = CiliaStub()
    o1 = await c.consultar_por_placa("PAN-1D24")
    o2 = await c.consultar_por_placa("PAN1D24")  # sem hífen
    assert o1 is not None
    assert o2 is not None
    assert o1.valor_total == o2.valor_total


@pytest.mark.asyncio
async def test_stub_simula_nao_encontrado():
    c = CiliaStub()
    o = await c.consultar_por_placa("XYZ-9999")
    assert o is None  # placas terminadas em 99 retornam None


@pytest.mark.asyncio
async def test_factory_usa_stub_por_default():
    c = build_cilia_client()
    assert isinstance(c, CiliaStub)
