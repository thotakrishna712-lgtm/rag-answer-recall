from __future__ import annotations

import copy
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from ragrecall.service import RecallService, build_service, load_jsonl

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)


@pytest.fixture()
def snapshot() -> list[dict[str, Any]]:
    return load_jsonl("data/cdc/snapshot.jsonl")


@pytest.fixture()
def updates() -> list[dict[str, Any]]:
    return load_jsonl("data/cdc/updates.jsonl")


@pytest.fixture()
def service(snapshot: list[dict[str, Any]]) -> Iterator[RecallService]:
    svc = build_service(":memory:", "data/entitlements.json")
    svc.apply_many(snapshot)
    yield svc
    svc.store.close()


def make_update(
    snapshot: list[dict[str, Any]], policy_id: str, lsn: int, **changes: Any
) -> dict[str, Any]:
    base = next(e for e in snapshot if e["payload"]["after"]["policy_id"] == policy_id)
    event = copy.deepcopy(base)
    before = copy.deepcopy(base["payload"]["after"])
    after = {**before, **changes}
    event["payload"].update({"op": "u", "before": before, "after": after})
    event["payload"]["source"]["lsn"] = lsn
    return event
