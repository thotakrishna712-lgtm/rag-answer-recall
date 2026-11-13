# Contributing

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
make lint test gate
```

- Branch from `main`, keep pull requests focused, and fill in the PR template.
- Every behaviour change needs a test. Changes to retrieval, chunking or data must keep
  `make gate` passing; if a golden answer legitimately changes, update
  `data/golden/golden.jsonl` in the same PR and say why.
- Spark changes: run `make spark-test` (Java 17) so the Spark planner stays in parity with
  the SQLite reference.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/).
