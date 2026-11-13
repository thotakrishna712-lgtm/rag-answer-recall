"""Debezium envelope parsing with reason-coded failures."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from ragrecall.models import CdcEvent


class QuarantineError(Exception):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


def _payload(raw: dict[str, Any]) -> dict[str, Any]:
    # Supports both the full {"schema":..., "payload":...} envelope and
    # schemas.enable=false converters that emit the payload directly.
    payload = raw.get("payload", raw)
    if not isinstance(payload, dict):
        raise QuarantineError("MALFORMED_ENVELOPE", "payload is not an object")
    return payload


def peek_lsn(raw: dict[str, Any]) -> int | None:
    try:
        payload = _payload(raw)
    except QuarantineError:
        return None
    source = payload.get("source") or {}
    lsn = source.get("lsn", payload.get("lsn"))
    return lsn if isinstance(lsn, int) else None


def peek_policy_id(raw: dict[str, Any]) -> str | None:
    try:
        payload = _payload(raw)
    except QuarantineError:
        return None
    for image in ("after", "before"):
        rec = payload.get(image)
        if isinstance(rec, dict) and isinstance(rec.get("policy_id"), str):
            return str(rec["policy_id"])
    return None


def parse_debezium(raw: dict[str, Any]) -> CdcEvent:
    payload = _payload(raw)
    source = payload.get("source") or {}
    candidate = {
        "op": payload.get("op"),
        "lsn": source.get("lsn", payload.get("lsn")),
        "ts_ms": payload.get("ts_ms", source.get("ts_ms")),
        "before": payload.get("before"),
        "after": payload.get("after"),
    }
    try:
        return CdcEvent.model_validate(candidate)
    except ValidationError as exc:
        first = exc.errors()[0]
        field = ".".join(str(p) for p in first["loc"])
        missing = first["type"] == "missing"
        code = "MISSING_REQUIRED_FIELD" if missing else "SCHEMA_VALIDATION_FAILED"
        raise QuarantineError(code, f"{field}: {first['msg']}") from exc
