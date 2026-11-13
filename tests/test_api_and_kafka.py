from __future__ import annotations

import json
from typing import Any

from fastapi.testclient import TestClient

from ragrecall.adapters.kafka_consumer import consume
from ragrecall.api import create_app
from ragrecall.service import RecallService, build_service

RETAIL = {"X-User-Groups": "retail_analyst"}


def test_api_end_to_end(service: RecallService, updates: list[Any]) -> None:
    client = TestClient(create_app(service))
    assert client.get("/health").json()["status"] == "ok"

    r = client.post(
        "/v1/ask",
        json={"question": "How long must wire transfer audit logs be retained?"},
        headers=RETAIL,
    )
    assert r.status_code == 200 and "7 years" in r.json()["answer"]
    answer_id = r.json()["answer_id"]

    results = client.post("/v1/cdc/events", json=updates).json()
    assert [x["status"] for x in results] == [
        "APPLIED",
        "SKIPPED_DUPLICATE",
        "SKIPPED_STALE",
        "QUARANTINED",
        "QUARANTINED",
        "APPLIED",
        "APPLIED",
    ]

    status = client.get(f"/v1/answers/{answer_id}").json()
    assert status["status"] == "RECALLED" and status["recalls"][0]["lsn"] == 200
    assert len(client.get("/v1/recalls").json()) == 1
    assert client.get("/v1/recalls", params={"since_id": 1}).json() == []

    history = client.get("/v1/policies/POL-WIRE-001/history").json()
    assert sum(1 for v in history if v["chunk_key"].endswith("audit-log-retention")) == 2
    assert {q["reason_code"] for q in client.get("/v1/quarantine").json()} == {
        "PII_DETECTED",
        "MISSING_REQUIRED_FIELD",
    }


def test_api_errors(service: RecallService) -> None:
    client = TestClient(create_app(service))
    assert client.post("/v1/ask", json={"question": "anything at all"}).status_code == 401
    assert client.get("/v1/answers/nope").status_code == 404
    assert client.get("/v1/policies/NOPE/history").status_code == 404
    assert client.post("/v1/ask", json={"question": "x"}, headers=RETAIL).status_code == 422


def test_app_factory_seeds_from_env(monkeypatch: Any) -> None:
    monkeypatch.setenv("RAGRECALL_DB", ":memory:")
    client = TestClient(create_app())
    assert client.get("/health").json()["index_generation"] == 4


class _Msg:
    def __init__(self, value: bytes | None, offset: int, err: str | None = None) -> None:
        self._v, self._o, self._e = value, offset, err

    def error(self) -> str | None:
        return self._e

    def value(self) -> bytes | None:
        return self._v

    def offset(self) -> int:
        return self._o


class _FakeConsumer:
    def __init__(self, messages: list[_Msg | None]) -> None:
        self.messages = messages
        self.committed: list[int] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.topics = topics

    def poll(self, timeout: float) -> _Msg | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, message: _Msg, asynchronous: bool) -> None:
        self.committed.append(message.offset())

    def close(self) -> None:
        self.closed = True


def test_kafka_consumer_commits_after_apply(snapshot: list[Any], updates: list[Any]) -> None:
    svc = build_service(":memory:", "data/entitlements.json")
    msgs: list[_Msg | None] = [_Msg(json.dumps(e).encode(), i) for i, e in enumerate(snapshot)]
    msgs += [
        None,
        _Msg(None, 4, err="broker down"),
        _Msg(json.dumps(updates[0]).encode(), 5),
        _Msg(None, 6),
    ]  # tombstone
    fake = _FakeConsumer(msgs)
    stats = consume(fake, svc, "dbserver.public.policies", max_idle_polls=2, poll_timeout=0)
    assert stats == {"processed": 5, "skipped_tombstones": 1, "errors": 1}
    assert fake.committed == [0, 1, 2, 3, 5, 6] and fake.closed
    assert (
        "10 years"
        in svc.ask("How long must wire transfer audit logs be retained?", ["retail_analyst"]).answer
    )
