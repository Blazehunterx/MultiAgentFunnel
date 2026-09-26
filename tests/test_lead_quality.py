#!/usr/bin/env python3
"""Unit tests for shared lead_quality insert gate (no DB, no network)."""

import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CB = os.path.join(BASE, "clawbuildr")
sys.path.insert(0, CB)
sys.path.insert(0, BASE)

from lead_quality import (
    validate_lead,
    is_generic_email,
    is_garbage_person_name,
    is_personal_email,
    is_placeholder_email,
    is_free_mail,
    is_business_tld,
)


def test_rejects_privacy_and_generic_emails():
    ok, reason = validate_lead("privacy@funda.nl", "Bedrijfspanden", "", "Funda", "fundainbusiness.nl")
    assert ok is False
    assert reason == "generic_email"

    ok, reason = validate_lead("groupprivacyofficer@sweco.se", "Sweco", "Wij", "Sweco", "sweco.nl")
    assert ok is False
    assert reason in ("generic_email", "non_business_tld")

    ok, reason = validate_lead("info@dksict.nl", "Firat", "", "Diensten", "dksict.nl")
    assert ok is False
    assert reason == "generic_email"

    ok, reason = validate_lead("support@clouditout.nl", "Eigenaar", "Novus", "Cloud", "clouditout.nl")
    assert ok is False


def test_rejects_page_title_first_names():
    ok, reason = validate_lead("veenendaal@mkbaccountants.nl", "Veenendaal", "", "MKB", "mkbaccountants.nl")
    assert ok is False
    assert reason == "garbage_name"

    ok, reason = validate_lead("jan@acme.nl", "Unknown", "", "Acme", "acme.nl")
    assert ok is False
    assert reason == "garbage_name"

    ok, reason = validate_lead("jan@acme.nl", "Home", "", "Acme", "acme.nl")
    assert ok is False


def test_accepts_real_personal_leads():
    ok, reason = validate_lead("jan.jansen@acme.nl", "Jan", "Jansen", "Acme", "acme.nl")
    assert ok is True
    assert reason == "ok"

    ok, reason = validate_lead("jan@acme.nl", "Jan", "", "Acme", "acme.nl")
    assert ok is True


def test_rejects_placeholders_free_mail_and_bad_tld():
    ok, reason = validate_lead("naam@voorbeeld.nl", "Jan", "Jansen", "Acme", "voorbeeld.nl")
    assert ok is False
    assert reason == "placeholder_email"

    ok, reason = validate_lead("jan@gmail.com", "Jan", "Jansen", "Acme", "gmail.com")
    assert ok is False
    assert reason in ("free_mail", "non_business_tld")

    ok, reason = validate_lead("x@y.zz", "Jan", "Jansen", "Acme", "y.zz")
    assert ok is False
    assert reason == "non_business_tld"


def test_generic_email_helpers():
    assert is_generic_email("info@x.nl") is True
    assert is_generic_email("privacy@x.nl") is True
    assert is_generic_email("dpo@x.nl") is True
    assert is_generic_email("info2@x.nl") is True
    assert is_generic_email("jan@x.nl") is False
    assert is_generic_email("anjavan.der@x.nl") is False
    assert is_personal_email("jan@acme.nl") is True
    assert is_personal_email("info@acme.nl") is False
    assert is_free_mail("a@b.nl") is False
    assert is_business_tld("acme.nl") is True
    assert is_business_tld("acme.se") is False


def test_garbage_company_not_fatal_by_default():
    # Personal email + page-title company: allowed, caller may fix company name
    ok, reason = validate_lead("jan@acme.nl", "Jan", "Jansen", "Home", "acme.nl")
    assert ok is True
    # Strict mode rejects
    ok2, reason2 = validate_lead("jan@acme.nl", "Jan", "Jansen", "Home", "acme.nl", require_good_company=True)
    assert ok2 is False
    assert reason2 == "garbage_company"


def test_is_garbage_person_name():
    assert is_garbage_person_name("Bedrijfspanden", "") is True
    assert is_garbage_person_name("Veenendaal", "") is True
    assert is_garbage_person_name("Eigenaar", "Novus") is True
    assert is_garbage_person_name("Jan", "Jansen") is False
    assert is_garbage_person_name("", "") is True


def test_placeholder_helper():
    assert is_placeholder_email("naam@voorbeeld.com") is True
    assert is_placeholder_email("jan@acme.nl") is False
