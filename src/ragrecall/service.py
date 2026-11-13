"""Application service: the only object the API, CLI and Kafka consumer talk to."""

from __future__ import annotations

import itertools
import json
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from ragrecall.generator import ExtractiveGenerator, Generator
from ragrecall.models import (
    AnswerStatus,
    ApplyResult,
    AskResponse,
    ChunkVersion,
    Citation,
    QuarantineRecord,
    RecallNotice,
)
from ragrecall.pipeline import apply_raw
from ragrecall.retriever import Retriever
from ragrecall.store import Store

REFUSAL = "No answer found in the policies available to your groups."


def load_entitlements(path: str | Path) -> dict[str, set[str]]:
    data = json.loads(Path(path).read_text())
    return {group: set(depts) for group, depts in data.items()}


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    lines = Path(path).read_text().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


class RecallService:
    def __init__(
        self,
        store: Store,
        entitlements: dict[str, set[str]],
        generator: Generator | None = None,
        min_score: float = 2.0,
        min_coverage: float = 0.5,
        top_k: int = 4,
        answer_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.store = store
        self.entitlements = entitlements
        self.retriever = Retriever(store)
        self.generator: Generator = generator or ExtractiveGenerator()
        self.min_score = min_score
        self.min_coverage = min_coverage
        self.top_k = top_k
        self._new_id = answer_id_factory or (lambda: uuid.uuid4().hex[:12])

    # ---- ingestion ---------------------------------------------------------------------
    def apply(self, raw: dict[str, Any]) -> ApplyResult:
        return apply_raw(self.store, raw)

    def apply_many(self, events: list[dict[str, Any]]) -> list[ApplyResult]:
        return [self.apply(e) for e in events]

    # ---- question answering --------------------------------------------------------------
    def allowed_departments(self, groups: list[str]) -> set[str]:
        allowed: set[str] = set()
        for g in groups:
            allowed |= self.entitlements.get(g, set())
        return allowed

    def ask(self, question: str, groups: list[str], record: bool = True) -> AskResponse:
        answer_id = self._new_id() if record else "not-recorded"
        hits = self.retriever.search(question, self.allowed_departments(groups), self.top_k)
        generation = None
        if (
            hits
            and hits[0].score >= self.min_score
            and self.retriever.coverage(question, hits[0]) >= self.min_coverage
        ):
            generation = self.generator.generate(question, hits, self.retriever.idf)

        if generation is None:
            text, used, refused = REFUSAL, [], True
        else:
            text, used, refused = generation.text, generation.used, False

        citations = [
            Citation(
                chunk_key=h.chunk_key,
                version_id=h.version_id,
                policy_id=h.policy_id,
                title=h.title,
                section=h.section,
                effective_date=date.fromisoformat(h.effective_date),
            )
            for h in used
        ]
        if record:
            self.store.record_answer(
                answer_id, question, groups, text, refused, [h.version_id for h in used]
            )
        return AskResponse(answer_id=answer_id, answer=text, refused=refused, citations=citations)

    # ---- recall & audit views --------------------------------------------------------------
    def recalls(self, since_id: int = 0, answer_id: str | None = None) -> list[RecallNotice]:
        notices = []
        for r in self.store.recalls(since_id, answer_id):
            replacement = self.store.current_for_key(r["chunk_key"])
            notices.append(
                RecallNotice(
                    recall_id=r["recall_id"],
                    answer_id=r["answer_id"],
                    question=r["question"],
                    chunk_key=r["chunk_key"],
                    reason=r["reason"],
                    lsn=r["lsn"],
                    superseded_text=r["superseded_text"],
                    replacement_text=replacement["text"] if replacement else None,
                )
            )
        return notices

    def answer_status(self, answer_id: str) -> AnswerStatus | None:
        row = self.store.answer(answer_id)
        if row is None:
            return None
        citations = [
            Citation(
                chunk_key=v["chunk_key"],
                version_id=v["version_id"],
                policy_id=v["policy_id"],
                title=v["title"],
                section=v["section"],
                effective_date=date.fromisoformat(v["effective_date"]),
            )
            for v in self.store.citations(answer_id)
        ]
        return AnswerStatus(
            answer_id=answer_id,
            question=row["question"],
            answer=row["answer_text"],
            status=row["status"],
            citations=citations,
            recalls=self.recalls(answer_id=answer_id),
        )

    def history(self, policy_id: str) -> list[ChunkVersion]:
        return [
            ChunkVersion(
                version_id=r["version_id"],
                chunk_key=r["chunk_key"],
                checksum=r["checksum"],
                valid_from_lsn=r["valid_from_lsn"],
                valid_to_lsn=r["valid_to_lsn"],
                is_current=bool(r["is_current"]),
                close_reason=r["close_reason"],
            )
            for r in self.store.history(policy_id)
        ]

    def quarantine(self) -> list[QuarantineRecord]:
        return [QuarantineRecord(**dict(r)) for r in self.store.quarantined()]


def sequential_ids(prefix: str = "ans") -> Callable[[], str]:
    """Deterministic answer ids for demos and reproducible docs."""
    counter = itertools.count(1)
    return lambda: f"{prefix}-{next(counter):04d}"


def build_service(
    db_path: str = ":memory:",
    entitlements_path: str | Path = "data/entitlements.json",
    seed_path: str | Path | None = None,
) -> RecallService:
    service = RecallService(Store(db_path), load_entitlements(entitlements_path))
    if seed_path and Path(seed_path).exists():
        service.apply_many(load_jsonl(seed_path))
    return service
