"""Optional LLM generator: answers strictly from retrieved chunks, with section citations.

    from ragrecall.adapters.openai_generator import OpenAIGenerator
    service = RecallService(store, entitlements, generator=OpenAIGenerator())

The evaluator's groundedness metric then becomes a real hallucination check, because
generated tokens are no longer guaranteed to come from the context.
"""

from __future__ import annotations

from collections.abc import Callable

from ragrecall.generator import Generation
from ragrecall.retriever import Hit

SYSTEM = (
    "Answer only from the numbered policy excerpts. Quote figures exactly. "
    "If the excerpts do not answer the question, reply exactly: INSUFFICIENT_CONTEXT."
)


class OpenAIGenerator:
    def __init__(self, model: str = "gpt-4o-mini", context_chunks: int = 3) -> None:
        from openai import OpenAI

        self.client = OpenAI()
        self.model = model
        self.context_chunks = context_chunks

    def generate(
        self, question: str, hits: list[Hit], weight: Callable[[str], float]
    ) -> Generation | None:
        used = hits[: self.context_chunks]
        context = "\n\n".join(
            f"[{i + 1}] {h.title} / {h.section}\n{h.text}" for i, h in enumerate(used)
        )
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": f"Excerpts:\n{context}\n\nQuestion: {question}"},
            ],
        )
        text = (resp.choices[0].message.content or "").strip()
        if not text or "INSUFFICIENT_CONTEXT" in text:
            return None
        return Generation(text, used)
