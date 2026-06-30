-- ============================================================================
-- SCRIPT 03 — FILE FORMATS + EXTERNAL STAGES
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   FILE FORMAT = a recipe telling Snowflake how to read your CSVs (comma-
--                 separated, has a header row, quotes, etc).
--   STAGE       = a "pointer" to a folder in S3 that Snowflake can read from,
--                 using the secure integration from script 02.
--
--   This project uses the GATEKEEPER pattern: a Python validator reads files
--   from the 3 S3 folders. So we create 3 stages — one per folder.
--
-- ⚠️ REPLACE <YOUR_BUCKET> with your S3 bucket name (3 places).
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA RAW;

-- ----------------------------------------------------------------------------
-- FILE FORMAT 1 — CSV WITH a header row (skip row 1).
--   Used for normal source files where the first line is column names.
-- ----------------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_DB.RAW.HEALTHCARE_CSV
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1                       -- ignore the header line
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'    -- handles quoted text with commas
  NULL_IF = ('', 'NULL')                -- treat blanks / "NULL" as NULL
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  COMMENT = 'CSV with header row';

-- ----------------------------------------------------------------------------
-- FILE FORMAT 2 — CSV WITHOUT a header row (read every line as data).
--   The gatekeeper loads into a temp table where it controls columns itself,
--   so it uses this no-header format.
-- ----------------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0                       -- no header — every row is data
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  COMMENT = 'CSV with no header row';

-- ----------------------------------------------------------------------------
-- STAGES — one pointer per S3 folder (built on the secure integration).
-- ----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS HEALTHCARE_DB.RAW.GK_INCOMING
  STORAGE_INTEGRATION = HEALTHCARE_S3_INT
  URL = 's3://<YOUR_BUCKET>/incoming/'
  FILE_FORMAT = HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER
  COMMENT = 'New files waiting to be validated';

CREATE STAGE IF NOT EXISTS HEALTHCARE_DB.RAW.GK_PROCESSED
  STORAGE_INTEGRATION = HEALTHCARE_S3_INT
  URL = 's3://<YOUR_BUCKET>/processed/'
  FILE_FORMAT = HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER
  COMMENT = 'Files that passed validation';

CREATE STAGE IF NOT EXISTS HEALTHCARE_DB.RAW.GK_QUARANTINE
  STORAGE_INTEGRATION = HEALTHCARE_S3_INT
  URL = 's3://<YOUR_BUCKET>/quarantine/'
  FILE_FORMAT = HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER
  COMMENT = 'Files that FAILED validation';

-- Let the operating role use the stages.
GRANT USAGE ON STAGE HEALTHCARE_DB.RAW.GK_INCOMING   TO ROLE HEALTHCARE_ROLE;
GRANT USAGE ON STAGE HEALTHCARE_DB.RAW.GK_PROCESSED  TO ROLE HEALTHCARE_ROLE;
GRANT USAGE ON STAGE HEALTHCARE_DB.RAW.GK_QUARANTINE TO ROLE HEALTHCARE_ROLE;

-- ----------------------------------------------------------------------------
-- TEST — confirm Snowflake can see your S3 folder. Should list any files there.
-- ----------------------------------------------------------------------------
LIST @HEALTHCARE_DB.RAW.GK_INCOMING;

-- Next: run 04_raw_tables.sql