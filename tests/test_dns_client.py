#!/usr/bin/env python3
"""Unit tests for clawbuildr.dns_client (UDP DNS with DNS-over-HTTPS fallback)
and the consumer call sites switched over to it. All network is mocked."""

import os
import sys

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import dns.resolver

import dns_client


class _FakeRR:
    """Minimal stand-in for a dnspython rdata object."""
    def __init__(self, text, **attrs):
        self._text = text
        for k, v in attrs.items():
            setattr(self, k, v)

    def __str__(self):
        return self._text


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = "json"

    def json(self):
        return self._payload


class _FakeHttpxClient:
    """Canned DoH JSON responses, consumed in order by .get()."""
    responses = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, headers=None):
        return _FakeResp(self.responses.pop(0))


@pytest.fixture(autouse=True)
def fresh_breaker(monkeypatch):
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", False)
    yield


def _udp_ok(records):
    def fake_resolve(name, rdtype, lifetime=None):
        return list(records)
    return fake_resolve


# ---------------------------------------------------------------------------
# resolve(): UDP path
# ---------------------------------------------------------------------------

def test_udp_success_returns_records(monkeypatch):
    recs = [_FakeRR("10 mx.nl.")]
    monkeypatch.setattr(dns.resolver, "resolve", _udp_ok(recs))
    out = dns_client.resolve("acme.nl", "MX")
    assert out == recs
    assert dns_client._UDP_BROKEN is False


def test_udp_nxdomain_reraised_and_breaker_not_tripped(monkeypatch):
    def fake_resolve(name, rdtype, lifetime=None):
        raise dns.resolver.NXDOMAIN()
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    with pytest.raises(dns.resolver.NXDOMAIN):
        dns_client.resolve("gone.example", "MX")
    assert dns_client._UDP_BROKEN is False


def test_udp_noanswer_reraised_and_breaker_not_tripped(monkeypatch):
    def fake_resolve(name, rdtype, lifetime=None):
        raise dns.resolver.NoAnswer()
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    with pytest.raises(dns.resolver.NoAnswer):
        dns_client.resolve("acme.nl", "MX")
    assert dns_client._UDP_BROKEN is False


def test_udp_timeout_trips_breaker_and_uses_doh(monkeypatch):
    def fake_resolve(name, rdtype, lifetime=None):
        raise RuntimeError("resolver unreachable")
    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [{"type": 15, "data": "10 mx.google.com."}]},
    ]
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(dns_client.httpx, "Client", _FakeHttpxClient)
    out = dns_client.resolve("hubspot.com", "MX")
    assert dns_client._UDP_BROKEN is True
    assert out[0].preference == 10
    assert str(out[0].exchange).rstrip(".") == "mx.google.com"


def test_breaker_skips_udp_on_second_lookup(monkeypatch):
    calls = {"n": 0}

    def fake_resolve(name, rdtype, lifetime=None):
        calls["n"] += 1
        raise RuntimeError("down")

    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [{"type": 15, "data": "5 a.nl."}]},
        {"Status": 0, "Answer": [{"type": 15, "data": "5 b.nl."}]},
    ]
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(dns_client.httpx, "Client", _FakeHttpxClient)
    dns_client.resolve("a.nl", "MX")
    dns_client.resolve("b.nl", "MX")
    assert calls["n"] == 1


def test_empty_name_raises_noanswer_without_network(monkeypatch):
    monkeypatch.setattr(dns_client.httpx, "Client",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network")))
    with pytest.raises(dns.resolver.NoAnswer):
        dns_client.resolve("", "MX")


# ---------------------------------------------------------------------------
# resolve(): DoH path
# ---------------------------------------------------------------------------

def test_doh_txt_parsed_into_rdata(monkeypatch):
    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [
            {"type": 16, "data": "\"v=spf1 include:_spf.google.com ~all\""}]},
    ]
    monkeypatch.setattr(dns_client.httpx, "Client", _FakeHttpxClient)
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", True)
    out = dns_client.resolve("acme.nl", "TXT")
    assert "v=spf1" in str(out[0])
    assert out[0].strings == (b"v=spf1 include:_spf.google.com ~all",)


def test_doh_nxdomain_status3(monkeypatch):
    _FakeHttpxClient.responses = [{"Status": 3}]
    monkeypatch.setattr(dns_client.httpx, "Client", _FakeHttpxClient)
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", True)
    with pytest.raises(dns.resolver.NXDOMAIN):
        dns_client.resolve("nx.example", "MX")


def test_doh_no_records_raises_noanswer(monkeypatch):
    _FakeHttpxClient.responses = [{"Status": 0, "Answer": [{"type": 1, "data": "1.2.3.4"}]}]
    monkeypatch.setattr(dns_client.httpx, "Client", _FakeHttpxClient)
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", True)
    with pytest.raises(dns.resolver.NoAnswer):
        dns_client.resolve("nomx.example", "MX")


def test_doh_second_provider_used_on_first_failure(monkeypatch):
    class _BadFirst(_FakeHttpxClient):
        def get(self, url, headers=None):
            if "dns.google" in url:
                return _FakeResp({"Status": 2})
            return _FakeResp({"Status": 0,
                              "Answer": [{"type": 15, "data": "20 mx.cf.nl."}]})

    monkeypatch.setattr(dns_client.httpx, "Client", _BadFirst)
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", True)
    out = dns_client.resolve("acme.nl", "MX")
    assert str(out[0].exchange).rstrip(".") == "mx.cf.nl"


def test_doh_total_failure_raises_timeout_error(monkeypatch):
    class _BothDown(_FakeHttpxClient):
        def get(self, url, headers=None):
            raise ConnectionError("offline")

    monkeypatch.setattr(dns_client.httpx, "Client", _BothDown)
    monkeypatch.setattr(dns_client, "_UDP_BROKEN", True)
    with pytest.raises(TimeoutError):
        dns_client.resolve("acme.nl", "MX")


# ---------------------------------------------------------------------------
# consumers
# ---------------------------------------------------------------------------

def _txt_rdata(text):
    import dns.rdata, dns.rdataclass, dns.rdatatype
    return dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.from_text("TXT"), text)


def test_deliverability_spf_found(monkeypatch):
    import clawbuildr_deliverability as cd
    monkeypatch.setattr(dns_client, "resolve",
                        lambda n, r, **k: [_txt_rdata("\"v=spf1 -all\"")])
    out = cd.check_spf("acme.nl")
    assert out["valid"] is True
    assert "v=spf1" in out["record"]


def test_deliverability_spf_nxdomain(monkeypatch):
    import clawbuildr_deliverability as cd

    def boom(n, r, **k):
        raise dns.resolver.NXDOMAIN()
    monkeypatch.setattr(dns_client, "resolve", boom)
    out = cd.check_spf("gone.example")
    assert out["valid"] is False
    assert out["error"] == "Domain not found"


def test_deliverability_mx_records(monkeypatch):
    import clawbuildr_deliverability as cd
    import dns.rdata, dns.rdataclass, dns.rdatatype
    mx = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.from_text("MX"), "1 smtp.google.com.")
    monkeypatch.setattr(dns_client, "resolve", lambda n, r, **k: [mx])
    out = cd.check_mx("acme.nl")
    assert out["valid"] is True
    assert out["records"][0]["priority"] == 1
    assert out["records"][0]["exchange"] == "smtp.google.com."


def test_lead_generator_verify_mx(monkeypatch):
    import clawbuildr_lead_generator as lg
    monkeypatch.setattr(dns_client, "resolve", lambda n, r, **k: [_FakeRR("10 mx.nl.")])
    assert lg.verify_mx("acme.nl") is True

    def boom(n, r, **k):
        raise dns.resolver.NXDOMAIN()
    monkeypatch.setattr(dns_client, "resolve", boom)
    assert lg.verify_mx("gone.example") is False


def test_tools_verify_email_no_mx_path_no_network(monkeypatch):
    """MX missing -> no SMTP attempt, graceful low-confidence result."""
    import tools

    def boom(n, r, **k):
        raise dns.resolver.NoAnswer()
    monkeypatch.setattr(dns_client, "resolve", boom)
    out = tools.verify_email("jan.devries@acme.nl")
    assert out["verified"] is False
    assert any("No MX records" in reason for reason in out["reasons"])


def test_tools_verify_email_placeholder_short_circuits(monkeypatch):
    import tools
    monkeypatch.setattr(dns_client, "resolve",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no DNS")))
    out = tools.verify_email("info@example.com")
    assert out["verified"] is False
    assert any("Placeholder" in reason for reason in out["reasons"])
