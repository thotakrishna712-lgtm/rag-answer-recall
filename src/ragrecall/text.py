"""Tokenisation shared by retrieval, generation and evaluation."""

from __future__ import annotations

import re

_STOP_WORDS = """
a an and are as at be been by can do does for from has have how i in is it its may must
of on or our should that the their this to was we what when where which who why will
with within you your
"""
_STOP = frozenset(_STOP_WORDS.split())  # noqa: SIM905
_TOKEN = re.compile(r"\$?\d[\d,.:]*|[a-z][a-z0-9-]*")
_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9$])")


def _normalise(token: str) -> str:
    token = token.rstrip(".,:")
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and not token.endswith("ss") and len(token) > 3:
        return token[:-1]
    return token


def tokenize(text: str) -> list[str]:
    return [
        _normalise(t) for t in _TOKEN.findall(text.lower()) if t not in _STOP and t.strip(".,:")
    ]


def sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENTENCE.split(text) if s.strip()]
