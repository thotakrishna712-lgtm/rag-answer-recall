# Changelog

## 0.1.0 - 2026-11-13

### Added
- Debezium CDC apply with LSN idempotency, stale-event handling and reason-coded quarantine
- PII guard (SSN, Luhn-checked cards, email) with redaction before storage
- Heading-aware chunker with stable section keys and checksums
- SCD Type 2 chunk versions with a one-current-row invariant
- Answer ledger and recall notices when cited versions are superseded or deleted
- Entitlement-filtered BM25 retrieval with query-coverage refusal
- Release gate that separates golden-set drift from regressions; optional MLflow logging
- Index reconciliation against source events
- FastAPI service, Kafka consumer with commit-after-apply, Redpanda compose stack
- Spark / Delta planner for Databricks with parity tests, Asset Bundle, Unity Catalog DDL
- Terraform S3 landing zone
