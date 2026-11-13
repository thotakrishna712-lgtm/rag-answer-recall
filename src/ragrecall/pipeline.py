"""Idempotent CDC apply: diff sections, version chunks, recall affected answers."""

from __future__ import annotations

import json
from typing import Any

from ragrecall.cdc import QuarantineError, parse_debezium, peek_lsn, peek_policy_id
from ragrecall.chunker import Chunk, chunk_policy
from ragrecall.models import ApplyResult, ApplyStatus, CdcEvent, CdcOp
from ragrecall.security import find_pii, redact
from ragrecall.store import Store, Tx


def apply_raw(store: Store, raw: dict[str, Any]) -> ApplyResult:
    """Entry point for a raw Debezium message (Kafka value, REST body, file line)."""
    lsn, policy_id = peek_lsn(raw), peek_policy_id(raw)
    if lsn is not None and store.is_applied(lsn):
        return ApplyResult(lsn=lsn, policy_id=policy_id, status=ApplyStatus.SKIPPED_DUPLICATE)
    try:
        event = parse_debezium(raw)
    except QuarantineError as q:
        return _quarantine(store, raw, lsn, policy_id, q.reason_code, q.detail)

    if event.after is not None:
        pii = find_pii(f"{event.after.title}\n{event.after.body_markdown}")
        if pii:
            return _quarantine(store, raw, lsn, policy_id, "PII_DETECTED", ",".join(pii))
    return apply_event(store, event)


def _quarantine(
    store: Store,
    raw: dict[str, Any],
    lsn: int | None,
    policy_id: str | None,
    reason: str,
    detail: str,
) -> ApplyResult:
    with store.transaction() as tx:
        tx.quarantine(lsn, reason, detail, redact(json.dumps(raw, sort_keys=True)))
        if lsn is not None:
            tx.mark_applied(lsn, policy_id, ApplyStatus.QUARANTINED)
    return ApplyResult(
        lsn=lsn, policy_id=policy_id, status=ApplyStatus.QUARANTINED, reason_code=reason
    )


def apply_event(store: Store, event: CdcEvent) -> ApplyResult:
    with store.transaction() as tx:
        if tx.is_applied(event.lsn):
            return ApplyResult(
                lsn=event.lsn, policy_id=event.policy_id, status=ApplyStatus.SKIPPED_DUPLICATE
            )
        state = tx.get_policy(event.policy_id)
        if state is not None and event.lsn <= state.last_lsn:
            # Late / replayed-from-older-offset event: the table already reflects newer state.
            tx.mark_applied(event.lsn, event.policy_id, ApplyStatus.SKIPPED_STALE)
            return ApplyResult(
                lsn=event.lsn, policy_id=event.policy_id, status=ApplyStatus.SKIPPED_STALE
            )

        result = _delete(tx, event) if event.op == CdcOp.DELETE else _upsert(tx, event)
        tx.mark_applied(event.lsn, event.policy_id, ApplyStatus.APPLIED)
        return result


def _delete(tx: Tx, event: CdcEvent) -> ApplyResult:
    before = event.before
    assert before is not None
    current = tx.current_chunks(event.policy_id)
    closed = {c.version_id: "POLICY_DELETED" for c in current.values()}
    tx.close_versions(closed, event.lsn)
    recalled = tx.recall_answers(closed, event.lsn)
    tx.upsert_policy(
        before.policy_id,
        before.title,
        before.department,
        before.owner,
        before.effective_date.isoformat(),
        event.lsn,
        is_deleted=True,
    )
    if closed:
        tx.bump_generation()
    return ApplyResult(
        lsn=event.lsn,
        policy_id=event.policy_id,
        status=ApplyStatus.APPLIED,
        removed=sorted(current),
        recalled_answer_ids=recalled,
    )


def _upsert(tx: Tx, event: CdcEvent) -> ApplyResult:
    after = event.after
    assert after is not None
    current = tx.current_chunks(event.policy_id)
    incoming: dict[str, Chunk] = {c.chunk_key: c for c in chunk_policy(after)}

    added = sorted(k for k in incoming if k not in current)
    removed = sorted(k for k in current if k not in incoming)
    modified = sorted(
        k for k in incoming if k in current and incoming[k].checksum != current[k].checksum
    )
    unchanged = sorted(
        k for k in incoming if k in current and incoming[k].checksum == current[k].checksum
    )

    closed = {current[k].version_id: "SECTION_MODIFIED" for k in modified}
    closed.update({current[k].version_id: "SECTION_REMOVED" for k in removed})
    tx.close_versions(closed, event.lsn)  # close before insert: one-current-row index
    tx.insert_versions([incoming[k] for k in added + modified], event.lsn)
    tx.refresh_metadata(after.policy_id, after.owner, after.effective_date.isoformat(), after.title)
    recalled = tx.recall_answers(closed, event.lsn)
    tx.upsert_policy(
        after.policy_id,
        after.title,
        after.department,
        after.owner,
        after.effective_date.isoformat(),
        event.lsn,
        is_deleted=False,
    )
    if added or modified or removed:
        tx.bump_generation()
    return ApplyResult(
        lsn=event.lsn,
        policy_id=event.policy_id,
        status=ApplyStatus.APPLIED,
        added=added,
        modified=modified,
        removed=removed,
        unchanged=unchanged,
        recalled_answer_ids=recalled,
    )
