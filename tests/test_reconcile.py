from __future__ import annotations

from typing import Any

from ragrecall.reconcile import actual_from_store, expected_from_events, main, reconcile
from ragrecall.service import RecallService


def test_store_reconciles_after_messy_release(
    service: RecallService, snapshot: list[Any], updates: list[Any]
) -> None:
    service.apply_many(updates)
    report = reconcile(expected_from_events(snapshot + updates), actual_from_store(service.store))
    assert report.passed, report
    assert report.source_count == report.target_count == 10


def test_detects_missing_unexpected_and_drift() -> None:
    report = reconcile({"a": "1", "b": "2", "c": "3"}, {"a": "1", "b": "X", "d": "4"})
    assert not report.passed
    assert report.missing_in_target == ["c"]
    assert report.unexpected_in_target == ["d"]
    assert report.checksum_mismatch == ["b"]


def test_detects_index_that_missed_an_update(
    service: RecallService, snapshot: list[Any], updates: list[Any]
) -> None:
    # Index never received the release: reconciliation against source must fail.
    report = reconcile(expected_from_events(snapshot + updates), actual_from_store(service.store))
    assert not report.passed
    assert report.checksum_mismatch == ["POL-WIRE-001::audit-log-retention"]
    assert report.unexpected_in_target == [
        "POL-INC-004::postmortem",
        "POL-INC-004::severity-1-response",
    ]


def test_cli() -> None:
    assert main(["--events", "data/cdc/snapshot.jsonl", "data/cdc/updates.jsonl"]) == 0
