#!/usr/bin/env python3
"""Unit tests for clawbuildr_quality_gate pure check functions (no DB)."""

import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

from clawbuildr_quality_gate import (
    _check_personalization,
    _check_spam_triggers,
    _check_tone,
    _check_structure,
    is_research_thin,
)


def test_research_thin_missing():
    assert is_research_thin(None) is True
    assert is_research_thin("") is True
    assert is_research_thin("   ") is True


def test_research_thin_short_summary():
    assert is_research_thin({"summary": "Te kort", "data_quality_score": 80}) is True


def test_research_thin_low_quality_score():
    assert is_research_thin({
        "summary": "Dit is een redelijk complete samenvatting van het bedrijf met meerdere bronnen en details.",
        "data_quality_score": 20,
    }) is True


def test_research_thin_json_string_ok():
    import json
    raw = json.dumps({
        "summary": "Dit is een redelijk complete samenvatting van het bedrijf met meerdere bronnen en details.",
        "data_quality_score": 75,
    })
    assert is_research_thin(raw) is False


def test_research_thin_complete_dict_ok():
    assert is_research_thin({
        "summary": "Dit is een redelijk complete samenvatting van het bedrijf met meerdere bronnen en details over diensten.",
        "data_quality_score": 75,
    }) is False


def test_research_thin_invalid_json_long_blob_ok():
    blob = "x" * 200
    assert is_research_thin(blob) is False
    assert is_research_thin("short junk") is True


def test_personalization_penalizes_leaked_ids():
    score, issues = _check_personalization(
        "Voor Acme",
        "Beste Jan, bij Acme helpen we met IT. Dit is een voldoende lange tekst om te scoren en te laten zien dat de email persoonlijk is geschreven.",
        {"company_name": "Acme", "first_name": "Jan", "domain": "acme.nl"},
    )
    assert score >= 70

    score_bad, issues_bad = _check_personalization(
        "Voor Acme",
        "Beste Jan, ref co_29d033e6 en prsp_abc123 genoemd in tekst.",
        {"company_name": "Acme", "first_name": "Jan", "domain": "acme.nl"},
    )
    assert score_bad < score
    assert any("ID leaked" in i for i in issues_bad)


def test_personalization_domain_in_subject():
    score, issues = _check_personalization(
        "acme.nl hosting",
        "Beste Jan, bij Acme denken we na over hosting en support voor jullie team vandaag.",
        {"company_name": "Acme", "first_name": "Jan", "domain": "acme.nl"},
    )
    assert any("subject line" in i for i in issues)


def test_spam_triggers_detected():
    score, issues = _check_spam_triggers(
        "Gratis offerte - beperkte tijd!",
        "Vandaag nog een korting op onze diensten, garanderen wij de laagste prijs."
    )
    assert score < 100
    assert any("Spam trigger" in i for i in issues)


def test_spam_clean_passes():
    score, issues = _check_spam_triggers(
        "Gesprek over jullie IT",
        "Zouden we volgende week 20 minuten kunnen inplannen om te sparren?"
    )
    assert score == 100.0
    assert issues == []


def test_tone_pushy_language():
    score, issues = _check_tone("Hallo", "U moet dit zeker doen. Ik bel u morgen.")
    assert any("Pushy" in i for i in issues)


def test_structure_min_paragraphs():
    score, issues = _check_structure("Een onderwerp genoeg", "Een regel zonder structuur hier.")
    assert any("paragraph" in i.lower() or "paragraphs" in i.lower() for i in issues)
