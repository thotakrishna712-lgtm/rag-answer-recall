"""End-to-end walkthrough: python -m ragrecall.demo"""

from __future__ import annotations

from ragrecall.evaluate import GoldenCase, run_gate
from ragrecall.models import ApplyResult
from ragrecall.service import RecallService, build_service, load_jsonl, sequential_ids


def _rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def _ask(svc: RecallService, question: str, groups: list[str]) -> str:
    resp = svc.ask(question, groups)
    print(f"[{','.join(groups)}] Q: {question}")
    print(f"  A: {resp.answer}")
    for c in resp.citations:
        print(f"     cite: {c.policy_id} / {c.section} (version {c.version_id})")
    print(f"     answer_id={resp.answer_id}")
    return resp.answer_id


def _show(result: ApplyResult) -> None:
    line = f"lsn={result.lsn} {result.policy_id} -> {result.status}"
    if result.reason_code:
        line += f" [{result.reason_code}]"
    if result.status == "APPLIED":
        line += (
            f"  modified={len(result.modified)} removed={len(result.removed)} "
            f"unchanged={len(result.unchanged)} recalled={result.recalled_answer_ids}"
        )
    print("  " + line)


def main() -> None:
    _rule("1. Initial Debezium snapshot (op=r)")
    svc = build_service(":memory:", "data/entitlements.json")
    svc._new_id = sequential_ids()  # stable ids so the README output is reproducible
    for r in svc.apply_many(load_jsonl("data/cdc/snapshot.jsonl")):
        print(f"  lsn={r.lsn} {r.policy_id}: {len(r.added)} chunks indexed")
    golden = [GoldenCase.model_validate(c) for c in load_jsonl("data/golden/golden.jsonl")]
    baseline = run_gate(svc, golden)
    print(f"  baseline gate: {'PASSED' if baseline.passed else 'BLOCKED'} {baseline.metrics}")

    _rule("2. Staff ask questions; every answer is ledgered with exact chunk versions")
    retention_id = _ask(
        svc, "How long must wire transfer audit logs be retained?", ["retail_analyst"]
    )
    dual_id = _ask(
        svc, "What wire transfer amount requires dual control approval?", ["retail_analyst"]
    )
    _ask(svc, "When must a suspicious activity report be filed?", ["retail_analyst"])
    _ask(svc, "When must a suspicious activity report be filed?", ["compliance_officer"])
    sev1_id = _ask(svc, "How quickly must a Severity 1 incident be acknowledged?", ["ops_engineer"])

    _rule("3. Release: CDC stream with replay, late event, PII leak, bad schema, delete")
    changed: set[str] = set()
    for r in svc.apply_many(load_jsonl("data/cdc/updates.jsonl")):
        _show(r)
        changed |= r.changed_keys

    _rule("4. Answer recall: who was told something that is no longer true?")
    for n in svc.recalls():
        print(f"  RECALL #{n.recall_id} answer={n.answer_id} reason={n.reason} lsn={n.lsn}")
        print(f"    question:    {n.question}")
        print(f"    was based on: {n.superseded_text}")
        print(f"    now says:     {n.replacement_text or '(section removed)'}")
    for aid in (retention_id, dual_id, sev1_id):
        status = svc.answer_status(aid)
        assert status is not None
        print(f"  answer {aid}: {status.status:<8} {status.question}")

    _rule("5. Same question after the change: new version is served")
    _ask(svc, "How long must wire transfer audit logs be retained?", ["retail_analyst"])

    _rule("6. Targeted release gate: only golden cases touched by this release")
    report = run_gate(svc, golden, changed, mode="targeted")
    print(f"  gate: {'PASSED' if report.passed else 'BLOCKED'} ({report.evaluated} cases)")
    print(f"  owner review required: {report.review_required}")
    print(f"  regressions: {report.regressions or 'none'}")

    _rule("7. Quarantine (PII redacted before storage)")
    for q in svc.quarantine():
        snippet = "123-45-6789" in q.raw_redacted
        print(f"  lsn={q.lsn} {q.reason_code}: {q.detail} | raw SSN stored: {snippet}")

    _rule("8. SCD2 history for POL-WIRE-001 audit-log-retention")
    for v in svc.history("POL-WIRE-001"):
        if v.chunk_key.endswith("audit-log-retention"):
            print(
                f"  v{v.version_id} lsn[{v.valid_from_lsn}, {v.valid_to_lsn}) "
                f"current={v.is_current} closed_by={v.close_reason} checksum={v.checksum}"
            )


if __name__ == "__main__":
    main()
