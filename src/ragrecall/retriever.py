"""Entitlement-filtered BM25 over the *current* chunk versions only."""

from __future__ import annotations

import math
import threading
from collections import Counter
from dataclasses import dataclass

from ragrecall.store import Store
from ragrecall.text import tokenize


@dataclass(frozen=True)
class Hit:
    version_id: int
    chunk_key: str
    policy_id: str
    title: str
    section: str
    department: str
    effective_date: str
    text: str
    score: float


@dataclass
class _Doc:
    hit: Hit
    tf: Counter[str]
    length: int


class Retriever:
    """Rebuilds lazily whenever the store's index generation moves (any CDC change)."""

    def __init__(self, store: Store, k1: float = 1.5, b: float = 0.75) -> None:
        self.store = store
        self.k1, self.b = k1, b
        self._lock = threading.Lock()
        self._generation = -1
        self._docs: list[_Doc] = []
        self._df: Counter[str] = Counter()
        self._avg_len = 1.0

    def _refresh(self) -> None:
        generation = self.store.generation()
        if generation == self._generation:
            return
        docs: list[_Doc] = []
        df: Counter[str] = Counter()
        for row in self.store.current_versions():
            hit = Hit(
                row["version_id"],
                row["chunk_key"],
                row["policy_id"],
                row["title"],
                row["section"],
                row["department"],
                row["effective_date"],
                row["text"],
                0.0,
            )
            # Section heading is weighted twice: it is the most precise signal of intent.
            tokens = tokenize(f"{row['title']} {row['section']} {row['section']} {row['text']}")
            tf = Counter(tokens)
            docs.append(_Doc(hit, tf, len(tokens)))
            df.update(tf.keys())
        self._docs, self._df = docs, df
        self._avg_len = (sum(d.length for d in docs) / len(docs)) if docs else 1.0
        self._generation = generation

    def idf(self, token: str) -> float:
        n = len(self._docs)
        df = self._df.get(token, 0)
        return math.log(1 + (n - df + 0.5) / (df + 0.5))

    def coverage(self, question: str, hit: Hit) -> float:
        """IDF-weighted share of the question's terms that the hit actually contains.

        BM25 alone will happily rank a chunk that shares one rare word ("incident")
        with the question; coverage is what lets the service refuse instead.
        """
        terms = set(tokenize(question))
        if not terms:
            return 0.0
        present = set(tokenize(f"{hit.title} {hit.section} {hit.text}"))
        total = sum(self.idf(t) for t in terms)
        return sum(self.idf(t) for t in terms if t in present) / total if total else 0.0

    def search(self, question: str, allowed_departments: set[str], top_k: int = 4) -> list[Hit]:
        with self._lock:
            self._refresh()
            terms = set(tokenize(question))
            scored: list[Hit] = []
            for doc in self._docs:
                # Entitlement filter is applied BEFORE scoring: restricted text is never
                # ranked, so it cannot leak through snippets, scores or ordering.
                if doc.hit.department not in allowed_departments:
                    continue
                score = 0.0
                for t in terms:
                    f = doc.tf.get(t, 0)
                    if f:
                        norm = self.k1 * (1 - self.b + self.b * doc.length / self._avg_len)
                        score += self.idf(t) * f * (self.k1 + 1) / (f + norm)
                if score > 0:
                    h = doc.hit
                    scored.append(
                        Hit(
                            h.version_id,
                            h.chunk_key,
                            h.policy_id,
                            h.title,
                            h.section,
                            h.department,
                            h.effective_date,
                            h.text,
                            round(score, 4),
                        )
                    )
            scored.sort(key=lambda h: (-h.score, h.chunk_key))
            return scored[:top_k]
