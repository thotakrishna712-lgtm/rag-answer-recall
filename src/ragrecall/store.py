"""SQLite-backed versioned store.

Tables mirror the Delta layout used in production (see docs/DATABRICKS.md):
  chunk_versions  SCD Type 2 history of every chunk, one current row per chunk_key
  answers         ledger of every answer served, with the exact chunk versions cited
  recalls         notices raised when a cited version is superseded
  quarantine      reason-coded rejects, PII redacted
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ragrecall.chunker import Chunk

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO meta (key, value) VALUES ('index_generation', 0);

CREATE TABLE IF NOT EXISTS policies (
    policy_id TEXT PRIMARY KEY, title TEXT NOT NULL, department TEXT NOT NULL,
    owner TEXT NOT NULL, effective_date TEXT NOT NULL,
    last_lsn INTEGER NOT NULL, is_deleted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS applied_events (
    lsn INTEGER PRIMARY KEY, policy_id TEXT, status TEXT NOT NULL, applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_key TEXT NOT NULL, policy_id TEXT NOT NULL, title TEXT NOT NULL,
    section TEXT NOT NULL, department TEXT NOT NULL, owner TEXT NOT NULL,
    effective_date TEXT NOT NULL, text TEXT NOT NULL, checksum TEXT NOT NULL,
    valid_from_lsn INTEGER NOT NULL, valid_to_lsn INTEGER,
    is_current INTEGER NOT NULL, close_reason TEXT
);
-- Invariant: at most one live version per chunk.
CREATE UNIQUE INDEX IF NOT EXISTS ux_current_chunk ON chunk_versions (chunk_key)
    WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS ix_chunk_policy ON chunk_versions (policy_id, is_current);

CREATE TABLE IF NOT EXISTS answers (
    answer_id TEXT PRIMARY KEY, question TEXT NOT NULL, user_groups TEXT NOT NULL,
    answer_text TEXT NOT NULL, refused INTEGER NOT NULL, status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS answer_citations (
    answer_id TEXT NOT NULL, version_id INTEGER NOT NULL,
    PRIMARY KEY (answer_id, version_id)
);
CREATE INDEX IF NOT EXISTS ix_citation_version ON answer_citations (version_id);

CREATE TABLE IF NOT EXISTS recalls (
    recall_id INTEGER PRIMARY KEY AUTOINCREMENT, answer_id TEXT NOT NULL,
    version_id INTEGER NOT NULL, chunk_key TEXT NOT NULL, reason TEXT NOT NULL,
    lsn INTEGER NOT NULL, created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT, lsn INTEGER, reason_code TEXT NOT NULL,
    detail TEXT NOT NULL, raw_redacted TEXT NOT NULL, received_at TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class CurrentChunk:
    version_id: int
    chunk_key: str
    checksum: str


@dataclass(frozen=True)
class PolicyState:
    policy_id: str
    last_lsn: int
    is_deleted: bool


class Tx:
    """Operations that must run inside a single write transaction."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def is_applied(self, lsn: int) -> bool:
        row = self.conn.execute("SELECT 1 FROM applied_events WHERE lsn = ?", (lsn,)).fetchone()
        return row is not None

    def mark_applied(self, lsn: int, policy_id: str | None, status: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO applied_events (lsn, policy_id, status, applied_at) "
            "VALUES (?, ?, ?, ?)",
            (lsn, policy_id, status, _now()),
        )

    def get_policy(self, policy_id: str) -> PolicyState | None:
        row = self.conn.execute(
            "SELECT policy_id, last_lsn, is_deleted FROM policies WHERE policy_id = ?",
            (policy_id,),
        ).fetchone()
        return PolicyState(row[0], row[1], bool(row[2])) if row else None

    def upsert_policy(
        self,
        policy_id: str,
        title: str,
        department: str,
        owner: str,
        effective_date: str,
        lsn: int,
        is_deleted: bool,
    ) -> None:
        self.conn.execute(
            """INSERT INTO policies (policy_id, title, department, owner, effective_date,
                                     last_lsn, is_deleted)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (policy_id) DO UPDATE SET
                 title = excluded.title, department = excluded.department,
                 owner = excluded.owner, effective_date = excluded.effective_date,
                 last_lsn = excluded.last_lsn, is_deleted = excluded.is_deleted""",
            (policy_id, title, department, owner, effective_date, lsn, int(is_deleted)),
        )

    def current_chunks(self, policy_id: str) -> dict[str, CurrentChunk]:
        rows = self.conn.execute(
            "SELECT version_id, chunk_key, checksum FROM chunk_versions "
            "WHERE policy_id = ? AND is_current = 1",
            (policy_id,),
        ).fetchall()
        return {r[1]: CurrentChunk(r[0], r[1], r[2]) for r in rows}

    def close_versions(self, version_ids: dict[int, str], lsn: int) -> None:
        self.conn.executemany(
            "UPDATE chunk_versions SET is_current = 0, valid_to_lsn = ?, close_reason = ? "
            "WHERE version_id = ? AND is_current = 1",
            [(lsn, reason, vid) for vid, reason in version_ids.items()],
        )

    def insert_versions(self, chunks: list[Chunk], lsn: int) -> None:
        self.conn.executemany(
            """INSERT INTO chunk_versions (chunk_key, policy_id, title, section, department,
                   owner, effective_date, text, checksum, valid_from_lsn, is_current)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
            [
                (
                    c.chunk_key,
                    c.policy_id,
                    c.title,
                    c.section,
                    c.department,
                    c.owner,
                    c.effective_date,
                    c.text,
                    c.checksum,
                    lsn,
                )
                for c in chunks
            ],
        )

    def refresh_metadata(self, policy_id: str, owner: str, effective_date: str, title: str) -> None:
        # Owner / effective-date / title edits do not change what an answer says,
        # so they update live rows in place instead of creating new versions.
        self.conn.execute(
            "UPDATE chunk_versions SET owner = ?, effective_date = ?, title = ? "
            "WHERE policy_id = ? AND is_current = 1",
            (owner, effective_date, title, policy_id),
        )

    def bump_generation(self) -> None:
        self.conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'index_generation'")

    def recall_answers(self, closed: dict[int, str], lsn: int) -> list[str]:
        if not closed:
            return []
        marks = ",".join("?" * len(closed))
        rows = self.conn.execute(
            f"""SELECT c.answer_id, c.version_id, v.chunk_key
                FROM answer_citations c
                JOIN answers a ON a.answer_id = c.answer_id
                JOIN chunk_versions v ON v.version_id = c.version_id
                WHERE c.version_id IN ({marks}) AND a.status = 'FRESH'""",
            list(closed),
        ).fetchall()
        now = _now()
        self.conn.executemany(
            "INSERT INTO recalls (answer_id, version_id, chunk_key, reason, lsn, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(a, v, k, closed[v], lsn, now) for a, v, k in rows],
        )
        answer_ids = sorted({r[0] for r in rows})
        self.conn.executemany(
            "UPDATE answers SET status = 'RECALLED' WHERE answer_id = ?",
            [(a,) for a in answer_ids],
        )
        return answer_ids

    def quarantine(self, lsn: int | None, reason_code: str, detail: str, raw_redacted: str) -> None:
        self.conn.execute(
            "INSERT INTO quarantine (lsn, reason_code, detail, raw_redacted, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (lsn, reason_code, detail, raw_redacted, _now()),
        )


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None, timeout=10
        )
        self._conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[Tx]:
        with self._lock:
            # IMMEDIATE takes the write lock up front so two consumers cannot both
            # pass the idempotency check for the same LSN.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield Tx(self._conn)
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def _query(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    # ---- reads -------------------------------------------------------------------------
    def generation(self) -> int:
        return int(self._query("SELECT value FROM meta WHERE key = 'index_generation'")[0][0])

    def current_versions(self) -> list[sqlite3.Row]:
        return self._query("SELECT * FROM chunk_versions WHERE is_current = 1 ORDER BY chunk_key")

    def version(self, version_id: int) -> sqlite3.Row | None:
        rows = self._query("SELECT * FROM chunk_versions WHERE version_id = ?", (version_id,))
        return rows[0] if rows else None

    def current_for_key(self, chunk_key: str) -> sqlite3.Row | None:
        rows = self._query(
            "SELECT * FROM chunk_versions WHERE chunk_key = ? AND is_current = 1", (chunk_key,)
        )
        return rows[0] if rows else None

    def history(self, policy_id: str) -> list[sqlite3.Row]:
        return self._query(
            "SELECT * FROM chunk_versions WHERE policy_id = ? ORDER BY chunk_key, version_id",
            (policy_id,),
        )

    def answer(self, answer_id: str) -> sqlite3.Row | None:
        rows = self._query("SELECT * FROM answers WHERE answer_id = ?", (answer_id,))
        return rows[0] if rows else None

    def citations(self, answer_id: str) -> list[sqlite3.Row]:
        return self._query(
            """SELECT v.* FROM answer_citations c
               JOIN chunk_versions v ON v.version_id = c.version_id
               WHERE c.answer_id = ? ORDER BY v.version_id""",
            (answer_id,),
        )

    def recalls(self, since_id: int = 0, answer_id: str | None = None) -> list[sqlite3.Row]:
        sql = """SELECT r.*, a.question, v.text AS superseded_text
                 FROM recalls r
                 JOIN answers a ON a.answer_id = r.answer_id
                 JOIN chunk_versions v ON v.version_id = r.version_id
                 WHERE r.recall_id > ?"""
        params: list[Any] = [since_id]
        if answer_id:
            sql += " AND r.answer_id = ?"
            params.append(answer_id)
        return self._query(sql + " ORDER BY r.recall_id", params)

    def quarantined(self) -> list[sqlite3.Row]:
        return self._query("SELECT * FROM quarantine ORDER BY id")

    def is_applied(self, lsn: int) -> bool:
        return bool(self._query("SELECT 1 FROM applied_events WHERE lsn = ?", (lsn,)))

    # ---- answer ledger -------------------------------------------------------------------
    def record_answer(
        self,
        answer_id: str,
        question: str,
        groups: list[str],
        text: str,
        refused: bool,
        version_ids: list[int],
    ) -> None:
        with self.transaction() as tx:
            tx.conn.execute(
                "INSERT INTO answers (answer_id, question, user_groups, answer_text, refused, "
                "status, created_at) VALUES (?, ?, ?, ?, ?, 'FRESH', ?)",
                (answer_id, question, ",".join(groups), text, int(refused), _now()),
            )
            tx.conn.executemany(
                "INSERT INTO answer_citations (answer_id, version_id) VALUES (?, ?)",
                [(answer_id, v) for v in version_ids],
            )
