"""Typed contracts shared by the CDC pipeline, retrieval layer, evaluator and API."""

from __future__ import annotations

from datetime import date, timedelta
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

EPOCH = date(1970, 1, 1)


class CdcOp(StrEnum):
    SNAPSHOT = "r"
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"


class PolicyRecord(BaseModel):
    """Row image of the source `policies` table as emitted by Debezium."""

    policy_id: str = Field(min_length=3)
    title: str = Field(min_length=1)
    department: str = Field(pattern=r"^[a-z_]+$")
    owner: str = Field(min_length=1)
    effective_date: date
    body_markdown: str = Field(min_length=20)

    @field_validator("effective_date", mode="before")
    @classmethod
    def _debezium_epoch_days(cls, value: Any) -> Any:
        # io.debezium.time.Date encodes DATE columns as days since the epoch.
        if isinstance(value, int) and not isinstance(value, bool):
            return EPOCH + timedelta(days=value)
        return value


class CdcEvent(BaseModel):
    op: CdcOp
    lsn: int = Field(ge=0, description="Source log position; used for idempotency and ordering")
    ts_ms: int
    before: PolicyRecord | None = None
    after: PolicyRecord | None = None

    @model_validator(mode="after")
    def _images_match_op(self) -> CdcEvent:
        if self.op == CdcOp.DELETE and self.before is None:
            raise ValueError("delete event requires a 'before' image")
        if self.op != CdcOp.DELETE and self.after is None:
            raise ValueError(f"op '{self.op}' requires an 'after' image")
        return self

    @property
    def policy_id(self) -> str:
        record = self.after or self.before
        assert record is not None  # guaranteed by validator
        return record.policy_id


class ApplyStatus(StrEnum):
    APPLIED = "APPLIED"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
    SKIPPED_STALE = "SKIPPED_STALE"
    QUARANTINED = "QUARANTINED"


class ApplyResult(BaseModel):
    lsn: int | None
    policy_id: str | None
    status: ApplyStatus
    reason_code: str | None = None
    added: list[str] = Field(default_factory=list)
    modified: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    unchanged: list[str] = Field(default_factory=list)
    recalled_answer_ids: list[str] = Field(default_factory=list)

    @property
    def changed_keys(self) -> set[str]:
        return set(self.modified) | set(self.removed)


class Citation(BaseModel):
    chunk_key: str
    version_id: int
    policy_id: str
    title: str
    section: str
    effective_date: date


class AskRequest(BaseModel):
    question: str = Field(min_length=5, max_length=500)


class AskResponse(BaseModel):
    answer_id: str
    answer: str
    refused: bool
    citations: list[Citation]


class RecallNotice(BaseModel):
    recall_id: int
    answer_id: str
    question: str
    chunk_key: str
    reason: str
    lsn: int
    superseded_text: str
    replacement_text: str | None


class AnswerStatus(BaseModel):
    answer_id: str
    question: str
    answer: str
    status: str
    citations: list[Citation]
    recalls: list[RecallNotice]


class QuarantineRecord(BaseModel):
    id: int
    lsn: int | None
    reason_code: str
    detail: str
    raw_redacted: str
    received_at: str


class ChunkVersion(BaseModel):
    version_id: int
    chunk_key: str
    checksum: str
    valid_from_lsn: int
    valid_to_lsn: int | None
    is_current: bool
    close_reason: str | None
