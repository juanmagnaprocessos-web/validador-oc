"""Testa extracao + normalizacao de IP pelo login_attempts.extrair_ip_real.

Defesas testadas:
  * Primeiro hop de X-Forwarded-For (formato "client, proxy1, proxy2")
  * Fallback para request.client.host quando sem XFF
  * Normalizacao IPv6 para /64 (impede queima de /64 inteiro)
  * Formato invalido nao quebra (salva raw)
"""
from __future__ import annotations

from types import SimpleNamespace

from app.services.login_attempts import _normalizar_ip, extrair_ip_real


def _mk_request(headers: dict[str, str] | None = None, client_host: str | None = None):
    """Minimo viavel pro `extrair_ip_real` — starlette Request tem
    `.headers` (dict-like com .get) e `.client.host`."""
    h = {k.lower(): v for k, v in (headers or {}).items()}
    headers_obj = SimpleNamespace(get=lambda k, default=None: h.get(k.lower(), default))
    client_obj = SimpleNamespace(host=client_host) if client_host else None
    return SimpleNamespace(headers=headers_obj, client=client_obj)


def test_xff_pega_primeiro_hop_ignorando_proxies():
    r = _mk_request(
        headers={"X-Forwarded-For": "203.0.113.1, 10.0.0.1, 172.16.0.1"},
        client_host="10.0.0.1",  # proxy interno — deve ser ignorado
    )
    assert extrair_ip_real(r) == "203.0.113.1"


def test_sem_xff_usa_client_host():
    r = _mk_request(headers={}, client_host="198.51.100.42")
    assert extrair_ip_real(r) == "198.51.100.42"


def test_sem_xff_nem_client_retorna_placeholder_sem_quebrar_sql():
    r = _mk_request(headers={}, client_host=None)
    assert extrair_ip_real(r) == "0.0.0.0"


def test_ipv6_normalizado_para_slash_64():
    # Mesmo /64 mas host diferentes → devem colapsar pro mesmo prefix
    r1 = _mk_request(headers={"X-Forwarded-For": "2001:db8:abcd:0012::1"})
    r2 = _mk_request(headers={"X-Forwarded-For": "2001:db8:abcd:0012::ffff"})
    assert extrair_ip_real(r1) == extrair_ip_real(r2)
    assert extrair_ip_real(r1) == "2001:db8:abcd:12::"


def test_ipv4_nao_eh_mascarado():
    r = _mk_request(headers={"X-Forwarded-For": "203.0.113.5"})
    assert extrair_ip_real(r) == "203.0.113.5"


def test_xff_vazio_ou_apenas_virgula_cai_pra_client_host():
    r = _mk_request(headers={"X-Forwarded-For": ","}, client_host="1.2.3.4")
    assert extrair_ip_real(r) == "1.2.3.4"


def test_normalizar_ip_trata_porta_ipv4():
    assert _normalizar_ip("203.0.113.1:54321") == "203.0.113.1"


def test_normalizar_ip_trata_colchetes_ipv6():
    # `[::1]:1234` → strip colchetes + porta → "::1" → /64
    res = _normalizar_ip("[2001:db8::1]:443")
    assert res == "2001:db8::"


def test_normalizar_ip_formato_invalido_retorna_raw():
    # Nao lanca excecao — auditoria preserva valor bruto pra forense
    assert _normalizar_ip("not-an-ip") == "not-an-ip"
