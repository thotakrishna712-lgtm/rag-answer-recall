"""PII detection and redaction applied before anything is chunked, indexed or quarantined."""

from __future__ import annotations

import re

_SSN = re.compile(r"\b(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b")
_CARD = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def _card_matches(text: str) -> list[str]:
    hits = []
    for m in _CARD.finditer(text):
        digits = re.sub(r"\D", "", m.group())
        if 13 <= len(digits) <= 19 and _luhn_ok(digits):
            hits.append(m.group())
    return hits


def find_pii(text: str) -> list[str]:
    """Return the PII categories present in `text` (empty list when clean)."""
    found = []
    if _SSN.search(text):
        found.append("US_SSN")
    if _card_matches(text):
        found.append("CARD_NUMBER")
    if _EMAIL.search(text):
        found.append("EMAIL_ADDRESS")
    return found


def redact(text: str) -> str:
    """Mask PII so quarantine tables never become a second copy of the leak."""
    text = _SSN.sub("***-**-****", text)
    for card in _card_matches(text):
        text = text.replace(card, "**** **** **** " + re.sub(r"\D", "", card)[-4:])
    return _EMAIL.sub("<redacted-email>", text)
