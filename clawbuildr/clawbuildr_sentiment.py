#!/usr/bin/env python3
"""
ClawBuildr Sentiment — Reply sentiment analysis.
More detailed than the basic classifier in reply_detector.
"""

import re
import json
from typing import Dict, Any


POSITIVE_PATTERNS = [
    r"interessant", r"geïnteresseerd", r"laten\s+we", r"afspraak", r"gesprek",
    r"bellen", r"\bcall\b", r"meeting", r"demo", r"ja\s+graag", r"\btop\b",
    r"\bgoed\b", r"perfect", r"geweldig", r"thanks", r"thank\s+you",
    r"appreciate", r"love\s+to", r"\bsure\b", r"sounds\s+good", r"let'?s\s+talk",
    r"schedule", r"available", r"ben\s+voorstander", r"prima", r"uitstekend",
]
NEGATIVE_PATTERNS = [
    r"niet\s+geïnteresseerd", r"afgemeld", r"\bstop\b", r"unsubscribe",
    r"\buit\b", r"geen\s+interesse", r"niet\s+relevant", r"stoppen",
    r"afmelden", r"no\s+thanks", r"not\s+interested", r"\bremove\b",
    r"opt\s*out", r"do\s+not\s+contact", r"geen\s+behoefte",
]
MEETING_PATTERNS = [
    r"afspraak", r"inplannen", r"kalender", r"calendly", r"\bmeet\b",
    r"\bcall\b", r"bellen", r"telefoon", r"datum", r"\btijd\b",
    r"\bwhen\b", r"schedule", r"\bbook\b", r"calendar", r"\bavailable\b",
    r"\bfree\b", r"\bslot\b", r"plannen", r"inroosteren",
]
OBJECTION_PATTERNS = [
    r"\bmaar\b", r"\bechter\b", r"probleem", r"\bkosten\b", r"\bduur\b",
    r"twijfel", r"\bhowever\b", r"\bbut\b", r"\bconcern\b", r"\bworry\b",
    r"expensive", r"\bprice\b", r"budget", r"te\s+duur",
]
URGENCY_PATTERNS = [
    r"deadline", r"haast", r"snel", r"nu\b", r"direct", r"asap",
    r"\burgent\b", r"\bright\s+away\b", r"zsm",
]


def analyze_sentiment(text: str) -> Dict[str, Any]:
    lower = text.lower()

    scores = {
        "positive": sum(1 for p in POSITIVE_PATTERNS if re.search(p, lower)),
        "negative": sum(1 for p in NEGATIVE_PATTERNS if re.search(p, lower)),
        "meeting_request": sum(1 for p in MEETING_PATTERNS if re.search(p, lower)),
        "objection": sum(1 for p in OBJECTION_PATTERNS if re.search(p, lower)),
        "urgency": sum(1 for p in URGENCY_PATTERNS if re.search(p, lower)),
    }

    max_category = max(scores, key=scores.get) if max(scores.values()) > 0 else "neutral"
    total = sum(scores.values()) or 1
    confidence = max(scores.values()) / total if total > 0 else 0

    return {
        "sentiment": max_category,
        "scores": scores,
        "confidence": round(confidence, 2),
        "is_interested": scores["positive"] > 0 or scores["meeting_request"] > 0,
        "wants_meeting": scores["meeting_request"] >= 2,
        "has_objection": scores["objection"] >= 2,
        "is_negative": scores["negative"] > 0,
    }


def extract_meeting_info(text: str) -> Dict[str, Any]:
    info = {"has_date": False, "has_time": False, "proposed_times": []}

    date_patterns = [
        r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}",
        r"\d{1,2}\s+(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december)",
        r"(maandag|dinsdag|woensdag|donderdag|vrijdag|zaterdag|zondag)",
        r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)",
    ]
    for p in date_patterns:
        if re.search(p, text, re.IGNORECASE):
            info["has_date"] = True
            break

    time_patterns = [
        r"\d{1,2}:\d{2}", r"\d{1,2}\s*(uur|u\b|am|pm)",
        r"'s\s*(ochtens|middags|avonds)",
    ]
    for p in time_patterns:
        if re.search(p, text, re.IGNORECASE):
            info["has_time"] = True
            break

    return info


if __name__ == "__main__":
    tests = [
        "Hoi Marvin, klinkt goed! Laten we volgende week een gesprek inplannen.",
        "Nee bedankt, we hebben hier geen behoefte aan.",
        "Het klinkt interessant maar de kosten zijn wel een punt.",
        "Prima, stuur maar een Calendly link.",
    ]
    for t in tests:
        result = analyze_sentiment(t)
        print(f"Text: {t[:60]}...")
        print(f"  Sentiment: {result['sentiment']} (confidence: {result['confidence']})")
        print(f"  Interested: {result['is_interested']}, Meeting: {result['wants_meeting']}")
        print()
