"""Generate Debezium-format sample events from data/policies/*.md.

    python scripts/build_sample_events.py

snapshot.jsonl  initial snapshot (op=r) of all policies
updates.jsonl   one release worth of changes, including the failure modes the
                pipeline must survive: replay, out-of-order, PII leak, bad schema.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
POLICIES = ROOT / "data" / "policies"
OUT = ROOT / "data" / "cdc"
EPOCH = date(1970, 1, 1)
BASE_TS = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def load(policy_id: str) -> dict[str, Any]:
    text = (POLICIES / f"{policy_id}.md").read_text()
    m = re.match(r"---\n(.*?)\n---\n(.*)", text, re.S)
    assert m, policy_id
    meta = dict(line.split(": ", 1) for line in m.group(1).splitlines())
    days = (date.fromisoformat(meta["effective_date"]) - EPOCH).days
    return {
        "policy_id": meta["policy_id"],
        "title": meta["title"],
        "department": meta["department"],
        "owner": meta["owner"],
        "effective_date": days,  # io.debezium.time.Date
        "body_markdown": m.group(2).strip(),
    }


def envelope(
    op: str, lsn: int, before: dict[str, Any] | None, after: dict[str, Any] | None
) -> dict[str, Any]:
    return {
        "schema": {"name": "dbserver.public.policies.Envelope"},
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": BASE_TS + lsn * 1000,
            "source": {
                "connector": "postgresql",
                "db": "policy_db",
                "schema": "public",
                "table": "policies",
                "lsn": lsn,
                "snapshot": op == "r",
            },
        },
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ids = sorted(p.stem for p in POLICIES.glob("*.md"))
    docs = {pid: load(pid) for pid in ids}
    snapshot = [envelope("r", 100 + i, None, docs[pid]) for i, pid in enumerate(ids)]

    wire_v1 = docs["POL-WIRE-001"]
    wire_v2 = {
        **wire_v1,
        "body_markdown": wire_v1["body_markdown"].replace(
            "retained for 7 years", "retained for 10 years"
        ),
    }
    wire_stale = {
        **wire_v1,
        "body_markdown": wire_v1["body_markdown"].replace(
            "retained for 7 years", "retained for 5 years"
        ),
    }
    acc_pii = {
        **docs["POL-ACC-003"],
        "body_markdown": docs["POL-ACC-003"]["body_markdown"]
        + "\n\n## 4. Example\nTest customer John Doe, SSN 123-45-6789, may be unmasked.",
    }
    aml_bad = {k: v for k, v in docs["POL-AML-002"].items() if k != "effective_date"}
    inc = docs["POL-INC-004"]
    acc_owner = {**docs["POL-ACC-003"], "owner": "Cloud Security Office"}

    updates = [
        envelope("u", 200, wire_v1, wire_v2),  # retention 7 -> 10 years
        envelope("u", 200, wire_v1, wire_v2),  # broker redelivery of the same event
        envelope("u", 150, wire_v1, wire_stale),  # late event from an older position
        envelope("u", 210, docs["POL-ACC-003"], acc_pii),  # PII pasted into a policy
        envelope("u", 220, docs["POL-AML-002"], aml_bad),  # missing effective_date
        envelope("u", 225, docs["POL-ACC-003"], acc_owner),  # metadata-only change
        envelope("d", 230, inc, None),  # runbook retired
    ]
    for name, rows in (("snapshot.jsonl", snapshot), ("updates.jsonl", updates)):
        (OUT / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
        print(f"wrote {len(rows)} events -> data/cdc/{name}")


if __name__ == "__main__":
    main()
