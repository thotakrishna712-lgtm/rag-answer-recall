.PHONY: install lint test spark-test gate reconcile demo api up events down

install:
	python -m pip install -e ".[dev]"

lint:
	ruff check src tests scripts databricks
	ruff format --check src tests scripts databricks
	mypy src

test:
	pytest --cov=ragrecall --cov-report=term-missing

spark-test:
	pytest tests/spark -v

reconcile:
	python -m ragrecall.reconcile --events data/cdc/snapshot.jsonl data/cdc/updates.jsonl

gate:
	python -m ragrecall.evaluate --apply data/cdc/updates.jsonl

demo:
	python -m ragrecall.demo

api:
	uvicorn --factory ragrecall.api:create_app --reload

up:
	docker compose up -d --build

events:
	docker compose run --rm producer

down:
	docker compose down -v
