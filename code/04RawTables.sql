-- ============================================================================
-- SCRIPT 04 — RAW (BRONZE) LANDING TABLES
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   Creates the 3 raw tables that hold the source data exactly as it arrives.
--   The gatekeeper validates each file, then loads good rows into these tables.
--
--   Note the 3 EXTRA metadata columns on every table:
--     file_name    = which source file this row came from
--     upload_dttm  = when the file landed in S3
--     load_dttm    = when Snowflake loaded the row
--   These give you full traceability ("where did this row come from?").
--
--   The gatekeeper stores raw dates as VARCHAR (text), because incoming files
--   sometimes have messy date formats. dbt's Silver layer parses them safely.
-- ============================================================================

USE ROLE HEALTHCARE_ROLE;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA RAW;

-- ----------------------------------------------------------------------------
-- TABLE 1 — PATIENT_ADMISSIONS  (one row per hospital admission)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.RAW.PATIENT_ADMISSIONS (
  admission_id      NUMBER(10,0),   -- unique admission number
  patient_id        NUMBER(10,0),   -- which patient
  doctor_id         VARCHAR(5),     -- D001–D200
  hospital_id       VARCHAR(5),     -- H01–H10
  admit_date        VARCHAR(20),    -- text; dbt parses to DATE
  department        VARCHAR(20),
  admission_type    VARCHAR(5),     -- EMG / URG / ELC
  diagnosis_code    VARCHAR(10),
  length_of_stay    NUMBER(4,0),    -- days
  readmission_flag  NUMBER(1,0),    -- 0 or 1
  -- metadata (added by the gatekeeper) --
  file_name         VARCHAR(500),
  upload_dttm       TIMESTAMP_NTZ,
  load_dttm         TIMESTAMP_NTZ
);

-- ----------------------------------------------------------------------------
-- TABLE 2 — TREATMENT_RECORDS  (one row per treatment)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.RAW.TREATMENT_RECORDS (
  treatment_id      NUMBER(10,0),
  admission_id      NUMBER(10,0),   -- links to PATIENT_ADMISSIONS
  doctor_id         VARCHAR(5),
  procedure_code    VARCHAR(10),
  treatment_date    VARCHAR(20),    -- text; dbt parses to DATE
  cost              NUMBER(12,2),
  outcome           VARCHAR(1),     -- P / F / S
  -- metadata --
  file_name         VARCHAR(500),
  upload_dttm       TIMESTAMP_NTZ,
  load_dttm         TIMESTAMP_NTZ
);

-- ----------------------------------------------------------------------------
-- TABLE 3 — INSURANCE_CLAIMS  (one row per claim)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS HEALTHCARE_DB.RAW.INSURANCE_CLAIMS (
  claim_id          NUMBER(10,0),
  admission_id      NUMBER(10,0),   -- links to PATIENT_ADMISSIONS
  insurance_id      VARCHAR(5),     -- I01–I15
  claim_amount      NUMBER(12,2),
  approved_amount   NUMBER(12,2),
  claim_status      VARCHAR(1),     -- P / A / R
  claim_date        VARCHAR(20),    -- text; dbt parses to DATE
  settle_date       VARCHAR(20),    -- text; dbt parses to DATE
  -- metadata --
  file_name         VARCHAR(500),
  upload_dttm       TIMESTAMP_NTZ,
  load_dttm         TIMESTAMP_NTZ
);

-- Confirm the 3 tables exist.
SHOW TABLES IN SCHEMA HEALTHCARE_DB.RAW;

-- Next: run 05_audit_tables.sql