#!/usr/bin/env python3
"""Unit tests for search_rotator filters (no network)."""

import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

import search_rotator as sr


def test_skip_domain_blocks_junk():
    assert sr._skip_domain("linkedin.com") is True
    assert sr._skip_domain("nl.linkedin.com") is True
    assert sr._skip_domain("wikipedia.org") is True
    assert sr._skip_domain("indeed.com") is True
    assert sr._skip_domain("") is True
    assert sr._skip_domain("it-vanberkel.nl") is False


def test_skip_domain_suffix_match():
    assert sr._skip_domain("sub.jobstreet.com") is True
    assert sr._skip_domain("nl.linkedin.com") is True
    # bare endswith must not false-positive on unrelated hosts
    assert sr._skip_domain("mylinkedin.com") is False
    assert sr._skip_domain("example-trustpilot.com") is False
    assert sr._skip_domain("trustpilot.com") is True


def test_skip_title_blocks_reference_spam():
    assert sr._skip_title("What is SaaS? - Dictionary") is True
    assert sr._skip_title("Top 10 IT companies in NL") is True
    assert sr._skip_title("Vacatures IT dienstverlener") is True
    assert sr._skip_title("") is True
    assert sr._skip_title("Acme IT Diensten B.V.") is False


def test_query_variations_include_site_exclusions():
    vars = sr._query_variations("IT dienstverlener Nederland")
    assert "IT dienstverlener Nederland" in vars
    assert any("linkedin.com" in v for v in vars)


def test_normalize_domain():
    assert sr._normalize_domain("https://www.example.com/path") == "example.com"
    assert sr._normalize_domain("not a url") == ""


def test_dead_engines_removed_from_list():
    """mojeek/brave/ddg/yandex/startpage waste time or inject captcha junk."""
    import inspect
    src = inspect.getsource(sr.search_web)
    assert '"name": "mojeek"' not in src
    assert '"name": "brave"' not in src
    assert '"name": "duckduckgo_lite"' not in src
    assert '"name": "yandex"' not in src
    assert '"name": "startpage"' not in src
    assert '"name": "google"' in src
    assert '"name": "bing"' in src
