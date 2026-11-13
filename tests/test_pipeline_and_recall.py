from __future__ import annotations

from typing import Any

from ragrecall.models import ApplyStatus
from ragrecall.service import RecallService
from tests.conftest import make_update

RETENTION_Q = "How long must wire transfer audit logs be retained?"


def _current_count(svc: RecallService) -> int:
    return len(svc.store.current_versions())


def test_snapshot_indexes_all_sections(service: RecallService) -> None:
    assert _current_count(service) == 12


def test_snapshot_replay_is_idempotent(service: RecallService, snapshot: list[Any]) -> None:
    results = service.apply_many(snapshot)
    assert {r.status for r in results} == {ApplyStatus.SKIPPED_DUPLICATE}
    assert _current_count(service) == 12


def test_update_reversions_only_changed_section(
    service: RecallService, snapshot: list[Any]
) -> None:
    body = snapshot[3]["payload"]["after"]["body_markdown"].replace("7 years", "10 years")
    r = service.apply(make_update(snapshot, "POL-WIRE-001", 300, body_markdown=body))
    assert r.status == ApplyStatus.APPLIED
    assert r.modified == ["POL-WIRE-001::audit-log-retention"]
    assert len(r.unchanged) == 3 and not r.added and not r.removed
    assert _current_count(service) == 12  # unique-current invariant holds


def test_out_of_order_event_is_stale(service: RecallService, snapshot: list[Any]) -> None:
    body = snapshot[3]["payload"]["after"]["body_markdown"]
    service.apply(make_update(snapshot, "POL-WIRE-001", 300, body_markdown=body + "\nNew."))
    late = service.apply(make_update(snapshot, "POL-WIRE-001", 250, body_markdown=body))
    assert late.status == ApplyStatus.SKIPPED_STALE


def test_answer_recalled_only_when_its_section_changes(
    service: RecallService, snapshot: list[Any]
) -> None:
    retention = service.ask(RETENTION_Q, ["retail_analyst"])
    dual = service.ask(
        "What wire transfer amount requires dual control approval?", ["retail_analyst"]
    )
    body = snapshot[3]["payload"]["after"]["body_markdown"].replace("7 years", "10 years")
    r = service.apply(make_update(snapshot, "POL-WIRE-001", 300, body_markdown=body))

    assert r.recalled_answer_ids == [retention.answer_id]
    stale = service.answer_status(retention.answer_id)
    fresh = service.answer_status(dual.answer_id)
    assert stale is not None and stale.status == "RECALLED"
    assert fresh is not None and fresh.status == "FRESH"
    notice = stale.recalls[0]
    assert "7 years" in notice.superseded_text
    assert notice.replacement_text is not None and "10 years" in notice.replacement_text

    again = service.ask(RETENTION_Q, ["retail_analyst"])
    assert "10 years" in again.answer
    assert again.citations[0].version_id != retention.citations[0].version_id


def test_recall_is_not_repeated(service: RecallService, snapshot: list[Any]) -> None:
    service.ask(RETENTION_Q, ["retail_analyst"])
    body = snapshot[3]["payload"]["after"]["body_markdown"]
    service.apply(make_update(snapshot, "POL-WIRE-001", 300, body_markdown=body + "\nA."))
    r2 = service.apply(
        make_update(snapshot, "POL-WIRE-001", 301, body_markdown=body.replace("7 years", "8 years"))
    )
    assert r2.recalled_answer_ids == []  # already recalled, no duplicate notice
    assert len(service.recalls()) == 1


def test_metadata_only_change_does_not_recall(service: RecallService, snapshot: list[Any]) -> None:
    ans = service.ask("When does break-glass production access expire?", ["ops_engineer"])
    r = service.apply(make_update(snapshot, "POL-ACC-003", 300, owner="New Owner"))
    assert r.status == ApplyStatus.APPLIED and not r.modified and not r.recalled_answer_ids
    status = service.answer_status(ans.answer_id)
    assert status is not None and status.status == "FRESH"


def test_department_move_recalls_and_reentitles(
    service: RecallService, snapshot: list[Any]
) -> None:
    ans = service.ask("When does break-glass production access expire?", ["ops_engineer"])
    r = service.apply(make_update(snapshot, "POL-ACC-003", 300, department="risk_compliance"))
    assert ans.answer_id in r.recalled_answer_ids
    assert service.ask("When does break-glass production access expire?", ["ops_engineer"]).refused


def test_delete_closes_everything(service: RecallService, updates: list[Any]) -> None:
    ans = service.ask("How quickly must a Severity 1 incident be acknowledged?", ["ops_engineer"])
    r = service.apply(updates[-1])
    assert r.removed == ["POL-INC-004::postmortem", "POL-INC-004::severity-1-response"]
    assert r.recalled_answer_ids == [ans.answer_id]
    assert service.ask(
        "How quickly must a Severity 1 incident be acknowledged?", ["ops_engineer"]
    ).refused


def test_pii_event_quarantined_and_redacted(service: RecallService, updates: list[Any]) -> None:
    r = service.apply(updates[3])
    assert r.status == ApplyStatus.QUARANTINED and r.reason_code == "PII_DETECTED"
    q = service.quarantine()[0]
    assert "123-45-6789" not in q.raw_redacted and "***-**-****" in q.raw_redacted
    assert service.apply(updates[3]).status == ApplyStatus.SKIPPED_DUPLICATE
    assert len(service.quarantine()) == 1
    assert "6789" not in str([v["text"] for v in service.store.current_versions()])


def test_failed_transaction_rolls_back(service: RecallService, snapshot: list[Any]) -> None:
    import pytest

    from ragrecall import pipeline

    def boom(*_: Any, **__: Any) -> None:
        raise RuntimeError("crash mid-apply")

    body = snapshot[3]["payload"]["after"]["body_markdown"].replace("7 years", "10 years")
    event = make_update(snapshot, "POL-WIRE-001", 300, body_markdown=body)
    original = pipeline.Tx.recall_answers
    pipeline.Tx.recall_answers = boom  # type: ignore[method-assign,assignment]
    try:
        with pytest.raises(RuntimeError):
            service.apply(event)
    finally:
        pipeline.Tx.recall_answers = original  # type: ignore[method-assign]
    assert not service.store.is_applied(300)
    assert "7 years" in service.ask(RETENTION_Q, ["retail_analyst"]).answer
    assert service.apply(event).status == ApplyStatus.APPLIED  # safe to redeliver
