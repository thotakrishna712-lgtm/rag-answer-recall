from __future__ import annotations

from datetime import date

import pytest

from ragrecall.cdc import QuarantineError, parse_debezium, peek_lsn
from ragrecall.models import CdcOp
from ragrecall.security import find_pii, redact

AFTER = {
    "policy_id": "POL-X-1",
    "title": "T",
    "department": "operations",
    "owner": "o",
    "effective_date": 20454,
    "body_markdown": "## A\nSome sufficiently long body text.",
}


def test_parses_full_envelope_and_epoch_days() -> None:
    raw = {"payload": {"op": "c", "ts_ms": 1, "after": AFTER, "source": {"lsn": 7}}}
    event = parse_debezium(raw)
    assert event.op == CdcOp.CREATE and event.lsn == 7
    assert event.after is not None and event.after.effective_date == date(2026, 1, 1)


def test_parses_unwrapped_payload() -> None:
    assert parse_debezium({"op": "r", "ts_ms": 1, "lsn": 3, "after": AFTER}).lsn == 3


def test_missing_field_is_reason_coded() -> None:
    after = {k: v for k, v in AFTER.items() if k != "effective_date"}
    with pytest.raises(QuarantineError) as exc:
        parse_debezium({"op": "u", "ts_ms": 1, "lsn": 1, "after": after})
    assert exc.value.reason_code == "MISSING_REQUIRED_FIELD"


def test_delete_without_before_image_rejected() -> None:
    with pytest.raises(QuarantineError) as exc:
        parse_debezium({"op": "d", "ts_ms": 1, "lsn": 1})
    assert exc.value.reason_code == "SCHEMA_VALIDATION_FAILED"


def test_malformed_envelope() -> None:
    assert peek_lsn({"payload": "oops"}) is None
    with pytest.raises(QuarantineError):
        parse_debezium({"payload": "oops"})


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("SSN 123-45-6789 on file", ["US_SSN"]),
        ("card 4111 1111 1111 1111", ["CARD_NUMBER"]),
        ("ref 4111 1111 1111 1112", []),  # fails Luhn: not a card
        ("mail jane.doe@example.com", ["EMAIL_ADDRESS"]),
        ("Retain logs for 7 years and approve above $10,000.", []),
    ],
)
def test_find_pii(text: str, expected: list[str]) -> None:
    assert find_pii(text) == expected


def test_redact_masks_everything() -> None:
    out = redact("SSN 123-45-6789 card 4111 1111 1111 1111 mail a@b.io")
    assert "123-45-6789" not in out and "4111 1111 1111 1111" not in out
    assert out.endswith("<redacted-email>") and "1111" in out  # last four retained
