"""FastAPI surface. Run: uvicorn --factory ragrecall.api:create_app"""

from __future__ import annotations

import os
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException

from ragrecall import __version__
from ragrecall.models import (
    AnswerStatus,
    ApplyResult,
    AskRequest,
    AskResponse,
    ChunkVersion,
    QuarantineRecord,
    RecallNotice,
)
from ragrecall.service import RecallService, build_service


def _groups(header: str | None) -> list[str]:
    # In production this comes from validated JWT claims, never from a raw header.
    return [g.strip() for g in (header or "").split(",") if g.strip()]


def create_app(service: RecallService | None = None) -> FastAPI:
    svc = service or build_service(
        os.environ.get("RAGRECALL_DB", ":memory:"),
        os.environ.get("RAGRECALL_ENTITLEMENTS", "data/entitlements.json"),
        os.environ.get("RAGRECALL_SEED", "data/cdc/snapshot.jsonl"),
    )
    app = FastAPI(title="RAG Answer Recall", version=__version__)
    app.state.service = svc

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "index_generation": svc.store.generation()}

    @app.post("/v1/ask", response_model=AskResponse)
    def ask(body: AskRequest, x_user_groups: Annotated[str | None, Header()] = None) -> AskResponse:
        groups = _groups(x_user_groups)
        if not groups:
            raise HTTPException(401, "X-User-Groups header is required")
        return svc.ask(body.question, groups)

    @app.get("/v1/answers/{answer_id}", response_model=AnswerStatus)
    def answer(answer_id: str) -> AnswerStatus:
        status = svc.answer_status(answer_id)
        if status is None:
            raise HTTPException(404, "answer not found")
        return status

    @app.post("/v1/cdc/events", response_model=list[ApplyResult])
    def cdc_events(events: list[dict[str, Any]]) -> list[ApplyResult]:
        return svc.apply_many(events)

    @app.get("/v1/recalls", response_model=list[RecallNotice])
    def recalls(since_id: int = 0) -> list[RecallNotice]:
        return svc.recalls(since_id)

    @app.get("/v1/policies/{policy_id}/history", response_model=list[ChunkVersion])
    def history(policy_id: str) -> list[ChunkVersion]:
        rows = svc.history(policy_id)
        if not rows:
            raise HTTPException(404, "policy not found")
        return rows

    @app.get("/v1/quarantine", response_model=list[QuarantineRecord])
    def quarantine() -> list[QuarantineRecord]:
        return svc.quarantine()

    return app
