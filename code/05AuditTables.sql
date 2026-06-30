-- ============================================================================
-- SCRIPT 05 — AUDIT / MONITORING TABLES
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   Real pipelines keep a "flight recorder" — logs of every file processed,
--   every quality check, every failure, and who gets alert emails. These tables
--   are written to by the gatekeeper and the Airflow DAGs.
--
--   Tables created here:
--     FILE_PROCESSING_LOG   — one row PER FILE (the "report card")
--     DQ_METRICS_LOG        — one row PER CHECK (the "answer sheet")
--     PIPELINE_ERROR_LOG    — one row per DAG/task failure
--     EMAIL_RECIPIENT_LOG   — who receives alert emails (seeded with 1 address)
-- ============================================================================

USE ROLE HEALTHCARE_ROLE;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA AUDIT;

-- ----------------------------------------------------------------------------
-- 1) FILE_PROCESSING_LOG — one row per file the gatekeeper handles.
--    Answers: "what happened to this file, and how many rows loaded?"
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.AUDIT.FILE_PROCESSING_LOG (
  file_name         VARCHAR(500),    -- the source file
  feed_type         VARCHAR(50),     -- admissions / treatments / claims
  status            VARCHAR(20),     -- LOADED or QUARANTINED
  rows_loaded       NUMBER(12,0),    -- rows that made it in
  checks_passed     NUMBER(5,0),     -- how many checks passed
  checks_failed     NUMBER(5,0),     -- how many checks failed
  failure_reason    VARCHAR(5000),   -- why it was quarantined (if it was)
  processed_at      TIMESTAMP_NTZ    -- when this happened
);

-- ----------------------------------------------------------------------------
-- 2) DQ_METRICS_LOG — one row per individual quality check.
--    Answers: "exactly which checks passed/failed for this file?" (compliance)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.AUDIT.DQ_METRICS_LOG (
  file_name         VARCHAR(500),
  feed_type         VARCHAR(50),
  check_name        VARCHAR(200),    -- name of the check
  check_tier        VARCHAR(20),     -- GATE / THRESHOLD / ADVISORY
  check_result      VARCHAR(10),     -- PASS / FAIL
  detail            VARCHAR(2000),   -- numbers / message
  checked_at        TIMESTAMP_NTZ
);

-- ----------------------------------------------------------------------------
-- 3) PIPELINE_ERROR_LOG — one row per DAG/task failure (written by Airflow).
--    Answers: "which task failed, when, and what was the error?"
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG (
  dag_id            VARCHAR(200),
  task_id           VARCHAR(200),
  status            VARCHAR(50),
  triggered_by      VARCHAR(100),
  attempt           VARCHAR(20),
  error_summary     VARCHAR(5000),
  failed_at         TIMESTAMP_NTZ
);

-- ----------------------------------------------------------------------------
-- 4) EMAIL_RECIPIENT_LOG — who gets alert emails.
--    ⚠️ NEVER truncate this table — it controls who is notified.
--    Replace the email below with your real alert inbox.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.AUDIT.EMAIL_RECIPIENT_LOG (
  recipient_email   VARCHAR(200),
  alert_type        VARCHAR(50),     -- e.g. DQ_FAILURE
  is_active         BOOLEAN,
  added_at          TIMESTAMP_NTZ
);

-- Seed one active recipient (only inserts if the table is empty, so re-running
-- this whole script is safe and won't create duplicates).
INSERT INTO HEALTHCARE_DB.AUDIT.EMAIL_RECIPIENT_LOG
  (recipient_email, alert_type, is_active, added_at)
SELECT '<YOUR_ALERT_EMAIL>', 'DQ_FAILURE', TRUE, CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM HEALTHCARE_DB.AUDIT.EMAIL_RECIPIENT_LOG);

-- Confirm all 4 audit tables exist.
SHOW TABLES IN SCHEMA HEALTHCARE_DB.AUDIT;

-- Next: run 06_email_integration.sql