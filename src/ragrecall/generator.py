"""Answer generation. The default is extractive so the project runs offline and every
word of an answer is traceable to a cited chunk; an LLM generator plugs in via the
`Generator` protocol (see adapters/openai_generator.py)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ragrecall.retriever import Hit
from ragrecall.text import sentences, tokenize


@dataclass(frozen=True)
class Generation:
    text: str
    used: list[Hit]


class Generator(Protocol):
    def generate(
        self, question: str, hits: list[Hit], weight: Callable[[str], float]
    ) -> Generation | None: ...


class ExtractiveGenerator:
    def __init__(self, max_sentences: int = 2, context_chunks: int = 3) -> None:
        self.max_sentences = max_sentences
        self.context_chunks = context_chunks

    def generate(
        self, question: str, hits: list[Hit], weight: Callable[[str], float]
    ) -> Generation | None:
        terms = set(tokenize(question))
        candidates: list[tuple[float, int, int, str, Hit]] = []
        for rank, hit in enumerate(hits[: self.context_chunks]):
            for pos, sentence in enumerate(sentences(hit.text)):
                overlap = terms & set(tokenize(sentence))
                if not overlap:
                    continue
                score = sum(weight(t) for t in overlap) + (0.5 if rank == 0 else 0.0)
                candidates.append((score, rank, pos, sentence, hit))
        if not candidates:
            return None
        candidates.sort(key=lambda c: (-c[0], c[1], c[2]))
        best_score, best_rank = candidates[0][0], candidates[0][1]
        # Supporting sentences must come from the same section as the best sentence:
        # mixing sections produces fluent but mis-cited answers.
        chosen = [c for c in candidates if c[1] == best_rank and c[0] >= 0.7 * best_score][
            : self.max_sentences
        ]
        chosen.sort(key=lambda c: c[2])  # keep reading order
        return Generation(" ".join(c[3] for c in chosen), [chosen[0][4]])
