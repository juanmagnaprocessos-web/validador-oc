"""Testes para ClubClient._normalizar_pedido_v1 — extracao de placa."""
from __future__ import annotations

from app.clients.club_client import ClubClient


def test_identificador_preservado_quando_ja_presente():
    raw = {"identificador": "ABC-1D23", "observacao": "PRISMA (2017) QQF-2C69"}
    out = ClubClient._normalizar_pedido_v1(raw)
    # Nao sobrescreve se ja tem identificador
    assert out["identificador"] == "ABC-1D23"


def test_extrai_placa_de_request_obs_mercosul():
    raw = {
        "identificador": None,
        "request": {"obs": "PRISMA (2017) QQF-2C69 — Cinza — 9BGK..."},
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out["identificador"] == "QQF-2C69"
    assert out["identifier"] == "QQF-2C69"


def test_extrai_placa_de_request_obs_antigo():
    raw = {
        "identificador": None,
        "request": {"obs": "GOL 1.0 ABC1234 2019 — Preto"},
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out["identificador"] == "ABC-1234"


def test_extrai_placa_de_observacao_raiz():
    raw = {
        "identificador": None,
        "observacao": "CIVIC 2020 — XYZ-9A87 — Prata",
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out["identificador"] == "XYZ-9A87"


def test_extrai_placa_de_cot_obs():
    raw = {
        "identificador": None,
        "cot_obs": "HB20 ano 2021, placa def2g45",
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out["identificador"] == "DEF-2G45"


def test_ml_sem_placa_mantem_identificador_none():
    # Payload real observado na OC 2044627 (MERCADO LIVRE)
    raw = {
        "identificador": None,
        "observacao": (
            '{"eventNumber":"1333433742","car":"CIVIC (2017 A 2021) EXL 2.0 16V FLEX 2017",'
            '"address":"FORMULA PECAS","pix":"CARTAO"}'
        ),
        "cot_obs": None,
        "request": None,
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out.get("identificador") is None


def test_prioridade_request_obs_sobre_observacao():
    raw = {
        "identificador": None,
        "request": {"obs": "PRISMA AAA1B23 2020"},
        "observacao": "GOL XYZ9999 2015",
    }
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out["identificador"] == "AAA-1B23"


def test_payload_sem_nenhuma_observacao():
    raw = {"identificador": None, "id_pedido": "999"}
    out = ClubClient._normalizar_pedido_v1(raw)
    assert out.get("identificador") is None
    # Outros campos preservados
    assert out["id_pedido"] == "999"
