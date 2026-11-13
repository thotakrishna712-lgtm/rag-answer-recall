"""Release gate with golden-set drift classification.

A failing golden case is not automatically a regression. If the section the case
expects was modified or removed by a CDC event in this release, the *expected answer*
may now be wrong, so the case is routed to the policy owner as GOLDEN_REVIEW_REQUIRED
instead of blocking the release. Only failures on untouched content block.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

from ragrecall.chunker import base_key
from ragrecall.service import RecallService, build_service, load_jsonl
from ragrecall.text import tokenize


class GoldenCase(BaseModel):
    id: str
    question: str
    user_groups: list[str]
    expected_chunk: str | None = None
    must_contain: list[str] = Field(default_factory=list)
    expect_refusal: bool = False


class CaseStatus(StrEnum):
    PASS = "PASS"
    REGRESSION = "REGRESSION"
    GOLDEN_REVIEW_REQUIRED = "GOLDEN_REVIEW_REQUIRED"


class CaseResult(BaseModel):
    id: str
    status: CaseStatus
    retrieval_hit: bool
    answer_correct: bool
    groundedness: float
    refused: bool
    answer: str
    cited_chunks: list[str]


class Thresholds(BaseModel):
    retrieval_hit_rate: float = 0.90
    answer_correctness: float = 0.90
    groundedness: float = 0.95


class GateReport(BaseModel):
    mode: str
    passed: bool
    evaluated: int
    metrics: dict[str, float]
    thresholds: Thresholds
    regressions: list[str]
    review_required: list[str]
    cases: list[CaseResult]


def groundedness(answer: str, context: str) -> float:
    answer_tokens = tokenize(answer)
    if not answer_tokens:
        return 0.0
    context_tokens = set(tokenize(context))
    return round(sum(t in context_tokens for t in answer_tokens) / len(answer_tokens), 4)


def evaluate_case(service: RecallService, case: GoldenCase, changed: set[str]) -> CaseResult:
    resp = service.ask(case.question, case.user_groups, record=False)
    cited = [base_key(c.chunk_key) for c in resp.citations]

    if case.expect_refusal:
        ok = resp.refused
        hit, correct, grounded = ok, ok, 1.0
    else:
        context = " ".join(
            v["text"] for c in resp.citations if (v := service.store.version(c.version_id))
        )
        hit = case.expected_chunk in cited
        correct = not resp.refused and all(
            m.lower() in resp.answer.lower() for m in case.must_contain
        )
        grounded = groundedness(resp.answer, context) if not resp.refused else 0.0
        ok = hit and correct and grounded >= 0.9

    if ok:
        status = CaseStatus.PASS
    elif case.expected_chunk and case.expected_chunk in changed:
        status = CaseStatus.GOLDEN_REVIEW_REQUIRED
    else:
        status = CaseStatus.REGRESSION
    return CaseResult(
        id=case.id,
        status=status,
        retrieval_hit=hit,
        answer_correct=correct,
        groundedness=grounded,
        refused=resp.refused,
        answer=resp.answer,
        cited_chunks=cited,
    )


def run_gate(
    service: RecallService,
    golden: list[GoldenCase],
    changed_chunk_keys: set[str] | None = None,
    mode: str = "full",
    thresholds: Thresholds | None = None,
) -> GateReport:
    thresholds = thresholds or Thresholds()
    changed = {base_key(k) for k in (changed_chunk_keys or set())}
    if mode == "targeted":
        golden = [c for c in golden if c.expected_chunk in changed]
    elif mode != "full":
        raise ValueError("mode must be 'full' or 'targeted'")

    results = [evaluate_case(service, c, changed) for c in golden]
    scored = [r for r in results if r.status != CaseStatus.GOLDEN_REVIEW_REQUIRED]
    n = len(scored)
    metrics = {
        "retrieval_hit_rate": round(sum(r.retrieval_hit for r in scored) / n, 4) if n else 1.0,
        "answer_correctness": round(sum(r.answer_correct for r in scored) / n, 4) if n else 1.0,
        "groundedness": round(sum(r.groundedness for r in scored) / n, 4) if n else 1.0,
    }
    regressions = [r.id for r in results if r.status == CaseStatus.REGRESSION]
    passed = not regressions and all(metrics[name] >= getattr(thresholds, name) for name in metrics)
    return GateReport(
        mode=mode,
        passed=passed,
        evaluated=len(results),
        metrics=metrics,
        thresholds=thresholds,
        regressions=regressions,
        review_required=[r.id for r in results if r.status == CaseStatus.GOLDEN_REVIEW_REQUIRED],
        cases=results,
    )


def _log_to_mlflow(report: GateReport, params: dict[str, str]) -> None:
    """Log the gate to MLflow when it is installed and a tracking URI is configured."""
    if not os.environ.get("MLFLOW_TRACKING_URI"):
        return
    try:
        import mlflow
    except ImportError:
        return
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT", "rag-answer-recall-gate"))
    with mlflow.start_run(run_name=f"gate-{report.mode}"):
        mlflow.log_params(params)
        mlflow.log_metrics(
            {
                **report.metrics,
                "regressions": len(report.regressions),
                "review_required": len(report.review_required),
            }
        )
        mlflow.log_dict(report.model_dump(mode="json"), "gate_report.json")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="RAG release gate with golden drift detection")
    p.add_argument("--golden", default="data/golden/golden.jsonl")
    p.add_argument("--snapshot", default="data/cdc/snapshot.jsonl")
    p.add_argument("--apply", default=None, help="CDC events for this release (jsonl)")
    p.add_argument("--entitlements", default="data/entitlements.json")
    p.add_argument("--mode", choices=["full", "targeted"], default="full")
    p.add_argument("--report", default="reports/gate_report.json")
    args = p.parse_args(argv)

    service = build_service(":memory:", args.entitlements, args.snapshot)
    changed: set[str] = set()
    if args.apply:
        for result in service.apply_many(load_jsonl(args.apply)):
            changed |= result.changed_keys

    golden = [GoldenCase.model_validate(c) for c in load_jsonl(args.golden)]
    report = run_gate(service, golden, changed, args.mode)

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report.model_dump(mode="json"), indent=2))
    _log_to_mlflow(report, {"mode": args.mode, "events": str(args.apply)})

    verdict = "PASSED" if report.passed else "BLOCKED"
    print(f"RELEASE GATE {verdict} ({args.mode}, {report.evaluated} cases)")
    for name, value in report.metrics.items():
        print(f"  {name:<20} {value:.3f}  (min {getattr(report.thresholds, name):.2f})")
    for cid in report.regressions:
        print(f"  REGRESSION              {cid}")
    for cid in report.review_required:
        print(f"  GOLDEN_REVIEW_REQUIRED  {cid}  (source section changed; owner must confirm)")
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
