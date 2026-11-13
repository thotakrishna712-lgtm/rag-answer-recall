# Mapping to Databricks

`src/ragrecall/store.py` + `pipeline.py` are the reference implementation;
`src/ragrecall/lakehouse.py` is the Spark / Delta implementation of the same rules, and
`tests/spark/test_lakehouse.py` checks they end in identical state.

| Local | Databricks |
|---|---|
| Debezium JSON from Kafka | Confluent topic read with Spark Structured Streaming |
| `apply_event` per message | `foreachBatch`: dedupe by key, keep max LSN, `MERGE` into `chunk_versions` |
| `applied_events` | streaming checkpoint + LSN column used in the `MERGE` condition (`s.lsn > t.last_lsn`) |
| `chunk_versions` (SCD2) | Delta table with Change Data Feed on, liquid clustered by `policy_id` |
| current versions | `WHERE is_current` view, the **source table for Mosaic AI Vector Search Delta Sync** |
| entitlement filter | Unity Catalog row filter on `department`, plus a metadata filter on the index |
| `quarantine` | reason-coded Delta table shared with source-system owners |
| `answers`, `answer_citations` | inference table or Delta table written by the serving endpoint |
| recall engine | job reading CDF of `chunk_versions` (`_change_type = 'update_postimage'` on close) joined to citations |
| release gate | `mlflow.evaluate` / Agent Evaluation job in a Databricks Asset Bundle, run by GitHub Actions |
