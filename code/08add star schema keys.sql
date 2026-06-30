-- ============================================================================
-- STAR SCHEMA PRIMARY KEY + FOREIGN KEY CONSTRAINTS  (Gold / MARTS layer)
-- ----------------------------------------------------------------------------
-- WHY: Snowflake does NOT enforce these keys, but Power BI and Cortex AI READ
--      them and AUTO-BUILD the table relationships. This makes the star schema
--      self-documenting for every downstream BI tool.
--
-- WHEN TO RUN: After a dbt run that REBUILDS the Gold tables (e.g.
--      `dbt run --full-refresh`). Normal incremental runs keep the keys.
--      Safe to re-run any time — every statement drops the key first.
--
-- RELY: tells Snowflake/BI tools to TRUST these keys for query optimisation.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA MARTS;

-- ----------------------------------------------------------------------------
-- 1) PRIMARY KEYS on the 5 dimensions  (the "one" side of each relationship)
-- ----------------------------------------------------------------------------
ALTER TABLE DIM_DOCTOR     DROP PRIMARY KEY;
ALTER TABLE DIM_DOCTOR     ADD PRIMARY KEY (DOCTOR_SK) RELY;

ALTER TABLE DIM_HOSPITAL   DROP PRIMARY KEY;
ALTER TABLE DIM_HOSPITAL   ADD PRIMARY KEY (HOSPITAL_SK) RELY;

ALTER TABLE DIM_INSURANCE  DROP PRIMARY KEY;
ALTER TABLE DIM_INSURANCE  ADD PRIMARY KEY (INSURANCE_SK) RELY;

ALTER TABLE DIM_PATIENT    DROP PRIMARY KEY;
ALTER TABLE DIM_PATIENT    ADD PRIMARY KEY (PATIENT_SK) RELY;

ALTER TABLE DIM_DATE       DROP PRIMARY KEY;
ALTER TABLE DIM_DATE       ADD PRIMARY KEY (DATE_SK) RELY;

-- ----------------------------------------------------------------------------
-- 2) FOREIGN KEYS on the 3 facts  (the "many" side -> points to each dim PK)
-- ----------------------------------------------------------------------------

-- fct_admissions -> 4 dimensions
ALTER TABLE FCT_ADMISSIONS DROP CONSTRAINT IF EXISTS FK_ADM_PATIENT;
ALTER TABLE FCT_ADMISSIONS ADD CONSTRAINT FK_ADM_PATIENT
  FOREIGN KEY (PATIENT_SK)  REFERENCES DIM_PATIENT   (PATIENT_SK)   RELY;

ALTER TABLE FCT_ADMISSIONS DROP CONSTRAINT IF EXISTS FK_ADM_DOCTOR;
ALTER TABLE FCT_ADMISSIONS ADD CONSTRAINT FK_ADM_DOCTOR
  FOREIGN KEY (DOCTOR_SK)   REFERENCES DIM_DOCTOR    (DOCTOR_SK)    RELY;

ALTER TABLE FCT_ADMISSIONS DROP CONSTRAINT IF EXISTS FK_ADM_HOSPITAL;
ALTER TABLE FCT_ADMISSIONS ADD CONSTRAINT FK_ADM_HOSPITAL
  FOREIGN KEY (HOSPITAL_SK) REFERENCES DIM_HOSPITAL  (HOSPITAL_SK)  RELY;

ALTER TABLE FCT_ADMISSIONS DROP CONSTRAINT IF EXISTS FK_ADM_DATE;
ALTER TABLE FCT_ADMISSIONS ADD CONSTRAINT FK_ADM_DATE
  FOREIGN KEY (DATE_SK)     REFERENCES DIM_DATE      (DATE_SK)      RELY;

-- fct_treatments -> 2 dimensions
ALTER TABLE FCT_TREATMENTS DROP CONSTRAINT IF EXISTS FK_TRT_DOCTOR;
ALTER TABLE FCT_TREATMENTS ADD CONSTRAINT FK_TRT_DOCTOR
  FOREIGN KEY (DOCTOR_SK)   REFERENCES DIM_DOCTOR    (DOCTOR_SK)    RELY;

ALTER TABLE FCT_TREATMENTS DROP CONSTRAINT IF EXISTS FK_TRT_DATE;
ALTER TABLE FCT_TREATMENTS ADD CONSTRAINT FK_TRT_DATE
  FOREIGN KEY (DATE_SK)     REFERENCES DIM_DATE      (DATE_SK)      RELY;

-- fct_claims -> 2 dimensions
ALTER TABLE FCT_CLAIMS     DROP CONSTRAINT IF EXISTS FK_CLM_INSURANCE;
ALTER TABLE FCT_CLAIMS     ADD CONSTRAINT FK_CLM_INSURANCE
  FOREIGN KEY (INSURANCE_SK) REFERENCES DIM_INSURANCE (INSURANCE_SK) RELY;

ALTER TABLE FCT_CLAIMS     DROP CONSTRAINT IF EXISTS FK_CLM_DATE;
ALTER TABLE FCT_CLAIMS     ADD CONSTRAINT FK_CLM_DATE
  FOREIGN KEY (DATE_SK)     REFERENCES DIM_DATE      (DATE_SK)      RELY;

-- ----------------------------------------------------------------------------
-- 3) VERIFY — see every key you just declared
-- ----------------------------------------------------------------------------
SHOW PRIMARY KEYS IN SCHEMA HEALTHCARE_DB.MARTS;
SHOW IMPORTED KEYS IN SCHEMA HEALTHCARE_DB.MARTS;   -- the foreign keys