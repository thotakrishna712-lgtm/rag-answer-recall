-- Unity Catalog DDL. Run once per environment (dev / prod catalogs).
CREATE SCHEMA IF NOT EXISTS ${catalog}.policy_rag;

CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.chunk_versions (
  version_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  chunk_key      STRING NOT NULL,
  policy_id      STRING NOT NULL,
  title          STRING,
  section        STRING,
  department     STRING NOT NULL,
  owner          STRING,
  effective_date STRING,
  text           STRING NOT NULL,
  checksum       STRING NOT NULL,
  valid_from_lsn BIGINT NOT NULL,
  valid_to_lsn   BIGINT,
  is_current     BOOLEAN NOT NULL,
  close_reason   STRING
)
CLUSTER BY (policy_id, is_current)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Vector Search Delta Sync source: only live text is ever embedded.
CREATE OR REPLACE VIEW ${catalog}.policy_rag.chunks_current AS
SELECT version_id, chunk_key, policy_id, title, section, department, effective_date, text
FROM ${catalog}.policy_rag.chunk_versions WHERE is_current;

CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.policies (
  policy_id STRING NOT NULL, title STRING, department STRING, owner STRING,
  effective_date STRING, last_lsn BIGINT NOT NULL, is_deleted BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.answers (
  answer_id STRING NOT NULL, question STRING, user_groups STRING, answer_text STRING,
  refused BOOLEAN, status STRING NOT NULL, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.answer_citations (
  answer_id STRING NOT NULL, version_id BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.recalls (
  answer_id STRING NOT NULL, version_id BIGINT NOT NULL, chunk_key STRING,
  reason STRING, lsn BIGINT, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.quarantine (
  lsn BIGINT, policy_id STRING, reason_code STRING, detail STRING, received_at TIMESTAMP
);

-- Entitlements: a user only reads chunks of departments their groups map to.
CREATE TABLE IF NOT EXISTS ${catalog}.policy_rag.group_departments (
  group_name STRING NOT NULL, department STRING NOT NULL
);
CREATE OR REPLACE FUNCTION ${catalog}.policy_rag.department_filter(department STRING)
RETURN EXISTS (
  SELECT 1 FROM ${catalog}.policy_rag.group_departments g
  WHERE g.department = department AND is_account_group_member(g.group_name)
);
ALTER TABLE ${catalog}.policy_rag.chunk_versions
  SET ROW FILTER ${catalog}.policy_rag.department_filter ON (department);
