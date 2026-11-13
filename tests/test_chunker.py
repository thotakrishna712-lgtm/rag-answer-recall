from __future__ import annotations

from datetime import date

from ragrecall.chunker import base_key, chunk_policy, slugify
from ragrecall.models import PolicyRecord


def _record(body: str, department: str = "operations") -> PolicyRecord:
    return PolicyRecord(
        policy_id="POL-T-1",
        title="Test Policy",
        department=department,
        owner="Owner",
        effective_date=date(2026, 1, 1),
        body_markdown=body,
    )


def test_slug_ignores_numbering() -> None:
    assert slugify("4.2 Audit Log Retention") == slugify("Audit Log Retention")
    assert slugify("1. Scope") == "scope"


def test_sections_become_stable_keys() -> None:
    chunks = chunk_policy(_record("# Title\n\n## 1. Scope\nAlpha text.\n## 2. Rules\nBeta."))
    assert [c.chunk_key for c in chunks] == ["POL-T-1::scope", "POL-T-1::rules"]


def test_renumbering_a_section_keeps_key_but_changes_checksum() -> None:
    a = chunk_policy(_record("## 1. Scope\nSame text here."))[0]
    b = chunk_policy(_record("## 2. Scope\nSame text here."))[0]
    assert a.chunk_key == b.chunk_key
    assert a.checksum != b.checksum  # heading text is part of what the user sees


def test_department_change_changes_checksum() -> None:
    a = chunk_policy(_record("## Scope\nSame text here.", "operations"))[0]
    b = chunk_policy(_record("## Scope\nSame text here.", "retail_banking"))[0]
    assert a.checksum != b.checksum


def test_long_section_windowed_with_overlap() -> None:
    words = " ".join(f"w{i}" for i in range(400))
    chunks = chunk_policy(_record(f"## Big\n{words}"), max_words=160, overlap=30)
    assert [c.chunk_key for c in chunks] == ["POL-T-1::big", "POL-T-1::big#p1", "POL-T-1::big#p2"]
    assert chunks[0].text.split()[-30:] == chunks[1].text.split()[:30]
    assert {base_key(c.chunk_key) for c in chunks} == {"POL-T-1::big"}


def test_duplicate_headings_disambiguated() -> None:
    chunks = chunk_policy(_record("## Notes\nOne.\n## Notes\nTwo."))
    assert [c.chunk_key for c in chunks] == ["POL-T-1::notes", "POL-T-1::notes-2"]
