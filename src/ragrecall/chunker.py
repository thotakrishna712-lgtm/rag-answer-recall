"""Section-aware chunking with stable keys and content checksums."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from ragrecall.models import PolicyRecord

_HEADING = re.compile(r"^(#{1,3})\s+(.+?)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class Chunk:
    chunk_key: str
    policy_id: str
    title: str
    section: str
    department: str
    owner: str
    effective_date: str
    text: str
    checksum: str


def slugify(heading: str) -> str:
    """'4.2 Audit Log Retention' -> 'audit-log-retention' (numbering is not identity)."""
    stripped = re.sub(r"^[\d.\s]+", "", heading)
    return re.sub(r"[^a-z0-9]+", "-", stripped.lower()).strip("-") or "section"


def _sections(body: str) -> list[tuple[str, str]]:
    matches = list(_HEADING.finditer(body))
    if not matches:
        return [("Overview", body.strip())]
    sections: list[tuple[str, str]] = []
    preamble = body[: matches[0].start()].strip()
    if preamble:
        sections.append(("Overview", preamble))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        text = body[m.end() : end].strip()
        if text:
            sections.append((m.group(2).strip(), text))
    return sections


def _windows(words: list[str], max_words: int, overlap: int) -> list[list[str]]:
    if len(words) <= max_words:
        return [words]
    step = max_words - overlap
    out = []
    for start in range(0, len(words), step):
        out.append(words[start : start + max_words])
        if start + max_words >= len(words):
            break
    return out


def checksum(department: str, heading: str, text: str) -> str:
    # Department is part of identity: moving a section to another business line changes
    # who may see it, so it must be treated as a new version (and recall old answers).
    material = f"{department}\x1f{heading}\x1f{text}".encode()
    return hashlib.sha256(material).hexdigest()[:16]


def chunk_policy(record: PolicyRecord, max_words: int = 160, overlap: int = 30) -> list[Chunk]:
    if overlap >= max_words:
        raise ValueError("overlap must be smaller than max_words")
    chunks: list[Chunk] = []
    seen: dict[str, int] = {}
    for heading, text in _sections(record.body_markdown):
        slug = slugify(heading)
        seen[slug] = seen.get(slug, 0) + 1
        if seen[slug] > 1:
            slug = f"{slug}-{seen[slug]}"
        for part, words in enumerate(_windows(text.split(), max_words, overlap)):
            body = " ".join(words)
            key = f"{record.policy_id}::{slug}" + (f"#p{part}" if part else "")
            chunks.append(
                Chunk(
                    chunk_key=key,
                    policy_id=record.policy_id,
                    title=record.title,
                    section=heading,
                    department=record.department,
                    owner=record.owner,
                    effective_date=record.effective_date.isoformat(),
                    text=body,
                    checksum=checksum(record.department, heading, body),
                )
            )
    return chunks


def base_key(chunk_key: str) -> str:
    """Strip the window suffix so golden cases can target a whole section."""
    return chunk_key.split("#", 1)[0]
