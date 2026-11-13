from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ragrecall.evaluate import CaseStatus, GoldenCase, groundedness, main, run_gate
from ragrecall.generator import Generation
from ragrecall.retriever import Hit
from ragrecall.service import RecallService, load_jsonl


def _golden() -> list[GoldenCase]:
    return [GoldenCase.model_validate(c) for c in load_jsonl("data/golden/golden.jsonl")]


def test_entitlements_filter_before_ranking(service: RecallService) -> None:
    q = "When must a suspicious activity report be filed?"
    assert service.ask(q, ["retail_analyst"]).refused
    ok = service.ask(q, ["compliance_officer"])
    assert not ok.refused and ok.citations[0].policy_id == "POL-AML-002"
    hits = service.retriever.search(q, {"retail_banking"})
    assert all(h.department == "retail_banking" for h in hits)


def test_unknown_group_gets_nothing(service: RecallService) -> None:
    assert service.ask("How long must audit logs be retained?", ["intern"]).refused


def test_eval_does_not_pollute_ledger(service: RecallService) -> None:
    run_gate(service, _golden())
    assert service.store._query("SELECT COUNT(*) FROM answers")[0][0] == 0


def test_baseline_gate_passes(service: RecallService) -> None:
    report = run_gate(service, _golden())
    assert report.passed and report.metrics["answer_correctness"] == 1.0


def test_release_changes_route_to_review_not_regression(
    service: RecallService, updates: list[Any]
) -> None:
    changed: set[str] = set()
    for r in service.apply_many(updates):
        changed |= r.changed_keys
    report = run_gate(service, _golden(), changed)
    assert report.passed
    assert report.review_required == ["g01-wire-retention", "g09-sev1-ack", "g10-postmortem"]
    targeted = run_gate(service, _golden(), changed, mode="targeted")
    assert targeted.evaluated == 3


class _WrongGenerator:
    def generate(
        self, question: str, hits: list[Hit], weight: Callable[[str], float]
    ) -> Generation | None:
        return Generation("Retention is 99 years.", hits[:1])


def test_real_regression_blocks_release(service: RecallService) -> None:
    service.generator = _WrongGenerator()
    report = run_gate(service, _golden())
    assert not report.passed
    assert "g01-wire-retention" in report.regressions
    case = next(c for c in report.cases if c.id == "g01-wire-retention")
    assert case.status == CaseStatus.REGRESSION and case.groundedness < 0.9


def test_groundedness_metric() -> None:
    assert groundedness("logs retained 7 years", "Logs must be retained for 7 years.") == 1.0
    assert groundedness("logs retained 99 years", "Logs retained for 7 years.") < 1.0
    assert groundedness("", "anything") == 0.0


def test_cli_exit_codes(tmp_path: Any) -> None:
    report = tmp_path / "gate.json"
    assert main(["--report", str(report)]) == 0
    assert main(["--apply", "data/cdc/updates.jsonl", "--report", str(report)]) == 0
    assert report.exists()
