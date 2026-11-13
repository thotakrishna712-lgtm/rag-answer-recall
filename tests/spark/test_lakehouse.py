"""Parity tests: the Spark planner must produce the same state as the SQLite reference.

Runs on plain local Spark (no Delta jars needed): plans are collected and applied to an
in-memory model of the Delta tables. Skipped when pyspark is not installed.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from ragrecall.lakehouse import parse_events, plan_batch, plan_recalls  # noqa: E402
from ragrecall.service import build_service, load_jsonl  # noqa: E402

CURRENT = "version_id long, chunk_key string, policy_id string, checksum string"
POLICIES = "policy_id string, last_lsn long"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ragrecall-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


class DeltaModel:
    """Minimal in-memory stand-in for chunk_versions + policies Delta tables."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.versions: dict[int, dict[str, Any]] = {}
        self.policies: dict[str, int] = {}
        self.next_id = 1

    def current_df(self) -> DataFrame:
        rows = [
            (vid, v["chunk_key"], v["policy_id"], v["checksum"])
            for vid, v in self.versions.items()
            if v["is_current"]
        ]
        return self.spark.createDataFrame(rows, CURRENT)

    def policies_df(self) -> DataFrame:
        return self.spark.createDataFrame(list(self.policies.items()), POLICIES)

    def run(self, lines: list[str]) -> dict[str, Any]:
        raw = self.spark.createDataFrame([(line,) for line in lines], "value string")
        plan = plan_batch(parse_events(raw), self.policies_df(), self.current_df())
        closes = [r.asDict() for r in plan.to_close.collect()]
        inserts = [r.asDict() for r in plan.to_insert.collect()]
        for c in closes:
            v = self.versions[c["version_id"]]
            if v["is_current"]:
                v.update(
                    is_current=False, valid_to_lsn=c["valid_to_lsn"], close_reason=c["close_reason"]
                )
        for row in inserts:
            self.versions[self.next_id] = {**row, "is_current": True}
            self.next_id += 1
        for p in plan.policy_updates.collect():
            if p["last_lsn"] > self.policies.get(p["policy_id"], -1):
                self.policies[p["policy_id"]] = p["last_lsn"]
        return {
            "plan": plan,
            "closes": closes,
            "inserts": inserts,
            "quarantine": sorted(r["reason_code"] for r in plan.quarantine.collect()),
        }

    def current_checksums(self) -> dict[str, str]:
        return {v["chunk_key"]: v["checksum"] for v in self.versions.values() if v["is_current"]}


def _lines(path: str) -> list[str]:
    return [json.dumps(e) for e in load_jsonl(path)]


def _reference_state() -> dict[str, str]:
    svc = build_service(":memory:", "data/entitlements.json", "data/cdc/snapshot.jsonl")
    svc.apply_many(load_jsonl("data/cdc/updates.jsonl"))
    return {r["chunk_key"]: r["checksum"] for r in svc.store.current_versions()}


def test_snapshot_then_release_matches_sqlite_reference(spark: SparkSession) -> None:
    model = DeltaModel(spark)
    first = model.run(_lines("data/cdc/snapshot.jsonl"))
    assert len(first["inserts"]) == 12 and not first["closes"]

    second = model.run(_lines("data/cdc/updates.jsonl"))
    assert model.current_checksums() == _reference_state()
    assert second["quarantine"] == ["MISSING_REQUIRED_FIELD", "PII_DETECTED"]
    reasons = sorted((c["chunk_key"], c["close_reason"]) for c in second["closes"])
    assert reasons == [
        ("POL-INC-004::postmortem", "POLICY_DELETED"),
        ("POL-INC-004::severity-1-response", "POLICY_DELETED"),
        ("POL-WIRE-001::audit-log-retention", "SECTION_MODIFIED"),
    ]
    assert [i["chunk_key"] for i in second["inserts"]] == ["POL-WIRE-001::audit-log-retention"]
    assert model.policies["POL-WIRE-001"] == 200  # late lsn=150 did not move the watermark


def test_replayed_batch_is_a_no_op(spark: SparkSession) -> None:
    model = DeltaModel(spark)
    model.run(_lines("data/cdc/snapshot.jsonl"))
    model.run(_lines("data/cdc/updates.jsonl"))
    before = model.current_checksums()
    replay = model.run(_lines("data/cdc/updates.jsonl"))
    assert not replay["closes"] and not replay["inserts"]
    assert replay["plan"].policy_updates.count() == 0
    assert model.current_checksums() == before


def test_late_event_in_later_batch_is_dropped(spark: SparkSession) -> None:
    model = DeltaModel(spark)
    model.run(_lines("data/cdc/snapshot.jsonl"))
    updates = _lines("data/cdc/updates.jsonl")
    model.run(updates[:1])  # lsn 200
    late = model.run(updates[2:3])  # lsn 150 arrives afterwards
    assert not late["closes"] and not late["inserts"]


def test_recall_plan_targets_only_fresh_answers_on_closed_versions(spark: SparkSession) -> None:
    model = DeltaModel(spark)
    model.run(_lines("data/cdc/snapshot.jsonl"))
    by_key = {v["chunk_key"]: vid for vid, v in model.versions.items()}
    retention = by_key["POL-WIRE-001::audit-log-retention"]
    dual = by_key["POL-WIRE-001::dual-control-approval"]
    result = model.run(_lines("data/cdc/updates.jsonl"))

    citations = spark.createDataFrame(
        [("a-retention", retention), ("a-dual", dual), ("a-old", retention)],
        "answer_id string, version_id long",
    )
    answers = spark.createDataFrame(
        [("a-retention", "FRESH"), ("a-dual", "FRESH"), ("a-old", "RECALLED")],
        T.StructType(
            [T.StructField("answer_id", T.StringType()), T.StructField("status", T.StringType())]
        ),
    )
    recalls = plan_recalls(result["plan"].to_close, citations, answers).collect()
    assert [(r["answer_id"], r["reason"], r["lsn"]) for r in recalls] == [
        ("a-retention", "SECTION_MODIFIED", 200)
    ]


def test_metadata_only_change_refreshes_in_place(spark: SparkSession) -> None:
    model = DeltaModel(spark)
    model.run(_lines("data/cdc/snapshot.jsonl"))
    owner_change = _lines("data/cdc/updates.jsonl")[5]  # lsn 225
    result = model.run([owner_change])
    assert not result["closes"] and not result["inserts"]
    owners = {r["owner"] for r in result["plan"].to_refresh.collect()}
    assert owners == {"Cloud Security Office"}
