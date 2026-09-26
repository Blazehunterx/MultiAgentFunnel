#!/usr/bin/env python3
"""Unit tests for the Gemini email-guess step in clawbuildr.email_finder.

All network is mocked: no Gemini HTTP, no DNS/MX, no SMTP, no Hunter.
"""

import json
import os
import sys

import pytest

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import email_finder as ef

# Capture the real resolver before the fixture swaps in stubs; resolver tests
# exercise this reference so they aren't affected by the fixture's patching.
_REAL_RESOLVE_MX = ef._resolve_mx_host


@pytest.fixture(autouse=True)
def gemini_env(monkeypatch):
    """Enable the feature with a fake key and stub every network touchpoint."""
    monkeypatch.setattr(ef, "_GEMINI_API_KEYS", ["fake-key"])
    monkeypatch.setattr(ef, "_GEMINI_EMAIL_GUESS_ENABLED", True)
    monkeypatch.setattr(ef, "_GEMINI_GUESS_CACHE", {})
    monkeypatch.setattr(ef, "_DNS_UDP_BROKEN", False)
    monkeypatch.setattr(ef, "_call_gemini_sync", lambda *a, **k: None)
    monkeypatch.setattr(ef, "_resolve_mx_host", lambda d: "mx.test.local")
    monkeypatch.setattr(
        ef,
        "smtp_verify_email",
        lambda email, **k: {"email": email, "exists": None,
                            "confidence": 50, "reason": "MX valid, SMTP verification blocked"},
    )
    yield


def _gemini_reply(candidates):
    return json.dumps({"candidates": candidates})


# ---------------------------------------------------------------------------
# _extract_json_obj
# ---------------------------------------------------------------------------

def test_extract_json_plain():
    assert ef._extract_json_obj('{"candidates": []}') == {"candidates": []}


def test_extract_json_fenced():
    raw = '```json\n{"candidates": [{"email": "a@b.com"}]}\n```'
    assert ef._extract_json_obj(raw)["candidates"][0]["email"] == "a@b.com"


def test_extract_json_wrapped_in_prose():
    raw = 'Sure! Here you go:\n{"candidates": []}\nHope that helps.'
    assert ef._extract_json_obj(raw) == {"candidates": []}


def test_extract_json_garbage():
    assert ef._extract_json_obj("") is None
    assert ef._extract_json_obj("no json here") is None


def test_extract_json_non_dict_ignored():
    assert ef._extract_json_obj("[1, 2, 3]") is None


# ---------------------------------------------------------------------------
# structural checks
# ---------------------------------------------------------------------------

def test_local_contains_name_variants():
    assert ef._local_contains_name("jan.devries", "Jan", "de Vries") is True
    assert ef._local_contains_name("jandevries", "Jan", "de Vries") is True
    assert ef._local_contains_name("vries.jan", "Jan", "de Vries") is True
    assert ef._local_contains_name("pietje.puk", "Jan", "de Vries") is False


def test_candidate_structure_ok_valid():
    assert ef._candidate_structure_ok("jan.devries@acme.nl", "Jan", "de Vries", "acme.nl") is True


def test_candidate_structure_ok_www_domain_stripped():
    assert ef._candidate_structure_ok("jan.devries@acme.nl", "Jan", "de Vries", "www.acme.nl") is True


def test_candidate_structure_ok_wrong_domain_rejected():
    assert ef._candidate_structure_ok("jan.devries@evil.com", "Jan", "de Vries", "acme.nl") is False


def test_candidate_structure_ok_generic_rejected():
    assert ef._candidate_structure_ok("info@acme.nl", "Jan", "de Vries", "acme.nl") is False


def test_candidate_structure_ok_disposable_rejected():
    assert ef._candidate_structure_ok("jan.devries@yopmail.com", "Jan", "de Vries", "yopmail.com") is False


def test_candidate_structure_ok_missing_name_rejected():
    assert ef._candidate_structure_ok("secretary@acme.nl", "Jan", "de Vries", "acme.nl") is False


def test_candidate_structure_ok_malformed_rejected():
    assert ef._candidate_structure_ok("not-an-email", "Jan", "de Vries", "acme.nl") is False


# ---------------------------------------------------------------------------
# _gemini_guess_email_impl tiers
# ---------------------------------------------------------------------------

def test_impl_smtp_verified_tier(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@acme.nl",
                                                        "confidence": 85, "reason": "common pattern"}]))
    monkeypatch.setattr(ef, "smtp_verify_email",
                        lambda email, **k: {"email": email, "exists": True,
                                            "confidence": 90, "reason": "Mailbox verified (RCPT TO 250)"})
    result = ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["confidence"] == 90
    assert result["source"] == "gemini_smtp_verified"
    assert result["verified"] is True
    assert "SMTP verified" in result["reason"]


def test_impl_pattern_mx_tier(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@acme.nl",
                                                        "confidence": 85, "reason": "standard pattern"}]))
    result = ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["confidence"] == 70
    assert result["source"] == "gemini_pattern_mx"
    assert result["verified"] is False


def test_impl_nonstandard_email_not_accepted_without_smtp(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.d@acme.nl",
                                                        "confidence": 95, "reason": "guess"}]))
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_second_candidate_wins(monkeypatch):
    calls = {"n": 0}

    def fake_smtp(email, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"email": email, "exists": False, "confidence": 85, "reason": "550"}
        return {"email": email, "exists": True, "confidence": 90, "reason": "250"}

    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([
                            {"email": "jan.devries@acme.nl", "confidence": 85, "reason": "a"},
                            {"email": "devries@acme.nl", "confidence": 80, "reason": "b"},
                        ]))
    monkeypatch.setattr(ef, "smtp_verify_email", fake_smtp)
    result = ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl")
    assert result["email"] == "devries@acme.nl"
    assert result["confidence"] == 90


def test_impl_all_smtp_false_returns_none(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@acme.nl",
                                                        "confidence": 85, "reason": "a"}]))
    monkeypatch.setattr(ef, "smtp_verify_email",
                        lambda email, **k: {"email": email, "exists": False,
                                            "confidence": 85, "reason": "550"})
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_low_confidence_rejected(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@acme.nl",
                                                        "confidence": 40, "reason": "weak"}]))
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_wrong_domain_filtered(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@evil.com",
                                                        "confidence": 95, "reason": "wrong"}]))
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_no_mx_returns_none(monkeypatch):
    monkeypatch.setattr(ef, "_domain_has_mx", lambda d: False)
    monkeypatch.setattr(ef, "_call_gemini_sync",
                        lambda *a, **k: _gemini_reply([{"email": "jan.devries@acme.nl",
                                                        "confidence": 85, "reason": "a"}]))
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_invalid_reply_returns_none(monkeypatch):
    monkeypatch.setattr(ef, "_call_gemini_sync", lambda *a, **k: "not json at all")
    assert ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl") is None


def test_impl_prompt_includes_domain_and_name(monkeypatch):
    seen = {}

    def fake_call(prompt, system_prompt="", **k):
        seen["prompt"] = prompt
        return _gemini_reply([])

    monkeypatch.setattr(ef, "_call_gemini_sync", fake_call)
    ef._gemini_guess_email_impl("Jan", "de Vries", "acme.nl")
    assert "acme.nl" in seen["prompt"]
    assert "Jan de Vries" in seen["prompt"]


# ---------------------------------------------------------------------------
# gemini_guess_email: gating + cache
# ---------------------------------------------------------------------------

def test_guess_disabled_flag(monkeypatch):
    monkeypatch.setattr(ef, "_GEMINI_EMAIL_GUESS_ENABLED", False)
    assert ef.gemini_guess_email("Jan", "de Vries", "acme.nl") is None


def test_guess_no_keys(monkeypatch):
    monkeypatch.setattr(ef, "_GEMINI_API_KEYS", [])
    assert ef.gemini_guess_email("Jan", "de Vries", "acme.nl") is None


def test_guess_missing_args():
    assert ef.gemini_guess_email("", "de Vries", "acme.nl") is None
    assert ef.gemini_guess_email("Jan", "", "acme.nl") is None
    assert ef.gemini_guess_email("Jan", "de Vries", "") is None


def test_guess_caches_result(monkeypatch):
    calls = {"n": 0}

    def fake_impl(*a, **k):
        calls["n"] += 1
        return {"email": "jan.devries@acme.nl", "confidence": 90,
                "source": "gemini_smtp_verified", "verified": True}

    monkeypatch.setattr(ef, "_gemini_guess_email_impl", fake_impl)
    first = ef.gemini_guess_email("Jan", "de Vries", "acme.nl")
    second = ef.gemini_guess_email("Jan", "de Vries", "acme.nl")
    assert calls["n"] == 1
    assert first == second


def test_guess_caches_misses(monkeypatch):
    calls = {"n": 0}

    def fake_impl(*a, **k):
        calls["n"] += 1
        return None

    monkeypatch.setattr(ef, "_gemini_guess_email_impl", fake_impl)
    assert ef.gemini_guess_email("Jan", "de Vries", "acme.nl") is None
    assert ef.gemini_guess_email("Jan", "de Vries", "acme.nl") is None
    assert calls["n"] == 1


def test_guess_impl_exception_returns_none(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(ef, "_gemini_guess_email_impl", boom)
    assert ef.gemini_guess_email("Jan", "de Vries", "acme.nl") is None


# ---------------------------------------------------------------------------
# find_personal_email integration
# ---------------------------------------------------------------------------

def test_hunter_match_beats_gemini(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search", lambda *a, **k: {
        "emails": [{"email": "jan.devries@acme.nl", "confidence": 95,
                    "first_name": "Jan", "last_name": "de Vries",
                    "position": "CEO", "linkedin": ""}],
        "pattern": "{first}.{last}", "confidence": 95, "source": "hunter.io_match",
    })
    monkeypatch.setattr(ef, "gemini_guess_email",
                        lambda *a, **k: pytest.fail("Gemini must not run after a Hunter person match"))
    result = ef.find_personal_email("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["source"] == "hunter.io"
    assert result["verified"] is True


def test_hunter_pattern_beats_gemini(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search", lambda *a, **k: {
        "emails": [{"email": "someone.else@acme.nl", "confidence": 90,
                    "first_name": "Someone", "last_name": "Else",
                    "position": "", "linkedin": ""}],
        "pattern": "{first}.{last}", "confidence": 90, "source": "hunter.io",
    })
    monkeypatch.setattr(ef, "gemini_guess_email",
                        lambda *a, **k: pytest.fail("Gemini must not run when Hunter has a pattern"))
    result = ef.find_personal_email("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["source"] == "hunter_pattern"
    assert result["confidence"] == 70


def test_gemini_used_when_hunter_empty(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search",
                        lambda *a, **k: {"emails": [], "pattern": None,
                                         "confidence": 0, "source": "all_keys_rate_limited_or_failed"})
    monkeypatch.setattr(ef, "gemini_guess_email", lambda *a, **k: {
        "email": "jan.devries@acme.nl", "confidence": 90,
        "source": "gemini_smtp_verified", "verified": True, "reason": "checked",
    })
    result = ef.find_personal_email("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["confidence"] == 90


def test_blind_pattern_fallback_when_gemini_none(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search",
                        lambda *a, **k: {"emails": [], "pattern": None,
                                         "confidence": 0, "source": "none"})
    monkeypatch.setattr(ef, "gemini_guess_email", lambda *a, **k: None)
    monkeypatch.setattr(ef, "_domain_has_mx", lambda d: True)
    result = ef.find_personal_email("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["source"] == "personal_pattern_mx"
    assert result["confidence"] == 65
    assert result["verified"] is False


def test_blind_pattern_fallback_no_mx(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search",
                        lambda *a, **k: {"emails": [], "pattern": None,
                                         "confidence": 0, "source": "none"})
    monkeypatch.setattr(ef, "gemini_guess_email", lambda *a, **k: None)
    monkeypatch.setattr(ef, "_domain_has_mx", lambda d: False)
    result = ef.find_personal_email("Jan", "de Vries", "acme.nl")
    assert result["email"] == "jan.devries@acme.nl"
    assert result["source"] == "personal_pattern_no_mx"
    assert result["confidence"] == 30


def test_missing_data_short_circuits(monkeypatch):
    monkeypatch.setattr(ef, "hunter_domain_search",
                        lambda *a, **k: pytest.fail("no lookup should happen without data"))
    result = ef.find_personal_email("", "de Vries", "acme.nl")
    assert result["source"] == "missing_data"
    assert result["confidence"] == 0


# ---------------------------------------------------------------------------
# _resolve_mx_host: UDP DNS with DNS-over-HTTPS fallback
# ---------------------------------------------------------------------------

class _FakeRR:
    def __init__(self, prio, host):
        self.preference = prio
        self.exchange = host


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _FakeHttpxClient:
    """Stands in for httpx.Client; serves canned DoH JSON responses."""
    responses = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, headers=None):
        return _FakeResp(self.responses.pop(0))


def test_mx_udp_success_picks_lowest_preference(monkeypatch):
    import dns.resolver

    def fake_resolve(name, rdtype, lifetime=None):
        assert name == "acme.nl" and rdtype == "MX"
        return [_FakeRR(10, "mx2.acme.nl."), _FakeRR(5, "mx1.nl.")]

    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    assert _REAL_RESOLVE_MX("acme.nl") == "mx1.nl"
    assert ef._DNS_UDP_BROKEN is False


def test_mx_udp_nxdomain_is_definitive(monkeypatch):
    import dns.resolver
    calls = {"n": 0}

    def fake_resolve(name, rdtype, lifetime=None):
        calls["n"] += 1
        raise dns.resolver.NXDOMAIN()

    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(ef.httpx, "Client",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("DoH must not run")))
    assert _REAL_RESOLVE_MX("gone.example") is None
    assert calls["n"] == 1
    assert ef._DNS_UDP_BROKEN is False


def test_mx_udp_timeout_falls_back_to_doh(monkeypatch):
    import dns.resolver

    def fake_resolve(name, rdtype, lifetime=None):
        raise RuntimeError("resolver unreachable")

    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [{"type": 15, "data": "10 mx.google.com."}]},
    ]
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(ef.httpx, "Client", _FakeHttpxClient)
    assert _REAL_RESOLVE_MX("acme.nl") == "mx.google.com"
    assert ef._DNS_UDP_BROKEN is True


def test_mx_circuit_breaker_skips_udp_after_timeout(monkeypatch):
    import dns.resolver
    calls = {"n": 0}

    def fake_resolve(name, rdtype, lifetime=None):
        calls["n"] += 1
        raise RuntimeError("down")

    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [{"type": 15, "data": "5 mx.a.nl."}]},
        {"Status": 0, "Answer": [{"type": 15, "data": "5 mx.b.nl."}]},
    ]
    monkeypatch.setattr(dns.resolver, "resolve", fake_resolve)
    monkeypatch.setattr(ef.httpx, "Client", _FakeHttpxClient)
    _REAL_RESOLVE_MX("a.nl")
    _REAL_RESOLVE_MX("b.nl")
    assert calls["n"] == 1  # second lookup went straight to DoH


def test_mx_doh_nxdomain_returns_none(monkeypatch):
    def explode(*a, **k):
        raise AssertionError("UDP disabled by prior breaker")

    _FakeHttpxClient.responses = [{"Status": 3}]
    monkeypatch.setattr(ef.httpx, "Client", _FakeHttpxClient)
    monkeypatch.setattr(ef, "_DNS_UDP_BROKEN", True)
    assert _REAL_RESOLVE_MX("nx.example") is None


def test_mx_doh_no_mx_records_returns_none(monkeypatch):
    _FakeHttpxClient.responses = [
        {"Status": 0, "Answer": [{"type": 1, "data": "1.2.3.4"}]},
    ]
    monkeypatch.setattr(ef.httpx, "Client", _FakeHttpxClient)
    monkeypatch.setattr(ef, "_DNS_UDP_BROKEN", True)
    assert _REAL_RESOLVE_MX("nomx.example") is None


def test_mx_doh_second_provider_used_on_first_failure(monkeypatch):
    class _BadFirst(_FakeHttpxClient):
        def get(self, url, headers=None):
            if "dns.google" in url:
                return _FakeResp({"Status": 2})
            return _FakeResp({"Status": 0,
                              "Answer": [{"type": 15, "data": "20 mx.cf.nl."}]})

    monkeypatch.setattr(ef.httpx, "Client", _BadFirst)
    monkeypatch.setattr(ef, "_DNS_UDP_BROKEN", True)
    assert _REAL_RESOLVE_MX("acme.nl") == "mx.cf.nl"


def test_mx_invalid_domain_rejected_without_network(monkeypatch):
    monkeypatch.setattr(ef.httpx, "Client",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network")))
    assert _REAL_RESOLVE_MX("bad domain!") is None
    assert _REAL_RESOLVE_MX("") is None
    assert _REAL_RESOLVE_MX(None) is None


def test_domain_has_mx_uses_resolver(monkeypatch):
    monkeypatch.setattr(ef, "_resolve_mx_host", lambda d: "mx.acme.nl")
    assert ef._domain_has_mx("acme.nl") is True
    monkeypatch.setattr(ef, "_resolve_mx_host", lambda d: None)
    assert ef._domain_has_mx("acme.nl") is False
