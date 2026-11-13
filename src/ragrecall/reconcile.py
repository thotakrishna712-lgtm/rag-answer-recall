"""Index reconciliation: does what the retriever can see match the source of truth?

Used at cutover (legacy index -> new index) and as a nightly control. Source of truth is
the latest valid row per policy (re-chunked with the production chunker); target is the
set of current chunk versions. Reports counts, missing, unexpected and drifted keys.

    python -m ragrecall.reconcile --events data/cdc/snapshot.jsonl data/cdc/updates.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from pydantic import BaseModel

from ragrecall.cdc import QuarantineError, parse_debezium
from ragrecall.chunker import chunk_policy
from ragrecall.models import CdcOp
from ragrecall.security import find_pii
from ragrecall.service import build_service, load_jsonl
from ragrecall.store import Store


class ReconciliationReport(BaseModel):
    source_count: int
    target_count: int
    missing_in_target: list[str]
    unexpected_in_target: list[str]
    checksum_mismatch: list[str]
    passed: bool


def expected_from_events(events: list[dict[str, Any]]) -> dict[str, str]:
    """Replay events naively (latest valid LSN per policy wins) -> expected chunk checksums."""
    latest: dict[str, tuple[int, Any]] = {}
    for raw in events:
        try:
            event = parse_debezium(raw)
        except QuarantineError:
            continue
        if event.after and find_pii(f"{event.after.title}\n{event.after.body_markdown}"):
            continue
        if event.policy_id not in latest or event.lsn > latest[event.policy_id][0]:
            latest[event.policy_id] = (event.lsn, event)
    expected: dict[str, str] = {}
    for _, event in latest.values():
        if event.op != CdcOp.DELETE and event.after is not None:
            expected.update({c.chunk_key: c.checksum for c in chunk_policy(event.after)})
    return expected


def actual_from_store(store: Store) -> dict[str, str]:
    return {r["chunk_key"]: r["checksum"] for r in store.current_versions()}


def reconcile(source: dict[str, str], target: dict[str, str]) -> ReconciliationReport:
    missing = sorted(set(source) - set(target))
    unexpected = sorted(set(target) - set(source))
    drift = sorted(k for k in set(source) & set(target) if source[k] != target[k])
    return ReconciliationReport(
        source_count=len(source),
        target_count=len(target),
        missing_in_target=missing,
        unexpected_in_target=unexpected,
        checksum_mismatch=drift,
        passed=not (missing or unexpected or drift),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Reconcile current index against source events")
    p.add_argument("--events", nargs="+", required=True)
    p.add_argument("--db", default=":memory:", help="existing store; default replays events")
    p.add_argument("--entitlements", default="data/entitlements.json")
    args = p.parse_args(argv)

    events = [e for path in args.events for e in load_jsonl(path)]
    service = build_service(args.db, args.entitlements)
    if args.db == ":memory:":
        service.apply_many(events)
    report = reconcile(expected_from_events(events), actual_from_store(service.store))
    print(json.dumps(report.model_dump(), indent=2))
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
