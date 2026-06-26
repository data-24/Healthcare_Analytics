-- ════════════════════════════════════════════════════════════════════════════
-- 01_snowflake_setup.sql
-- Run this ONCE in a Snowflake Snowsight Worksheet, signed in as ACCOUNTADMIN.
-- It builds everything your profiles.yml points to.
-- ════════════════════════════════════════════════════════════════════════════
USE ROLE ACCOUNTADMIN;
---use warehouse HEALTHCARE_WH;
---use database healthcare_db;

-- ── 1) Warehouse (compute) ──────────────────────────────────────────────────

CREATE WAREHOUSE HEALTHCARE_WH 
WAREHOUSE_SIZE = 'SMALL'
AUTO_SUSPEND = 60        -- sleeps after 60s idle (saves credits)
AUTO_RESUME = TRUE;


-- ── 2) Database + medallion schemas ─────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS HEALTHCARE_DB;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.RAW;               -- Bronze (Snowpipe target)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.STAGING;           -- Silver (dbt views)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.MARTS;             -- Gold (dbt tables)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.SNAPSHOTS;         -- SCD2 history
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.AUDIT;             -- audit / reconciliation / errors
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.HEALTHCARE_SCHEMA; -- default schema in profiles.yml

-- ── 3) Role for dbt ─────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS HEALTHCARE_ROLE;

-- Compute + database access
GRANT USAGE ON WAREHOUSE HEALTHCARE_WH TO ROLE HEALTHCARE_ROLE;
GRANT USAGE        ON DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT CREATE SCHEMA ON DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;

-- Full control of every schema dbt builds into (current + future)
GRANT ALL PRIVILEGES ON ALL SCHEMAS    IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;

-- Read everything that lands in the database (incl. bronze tables created later)
GRANT SELECT ON ALL TABLES     IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT SELECT ON FUTURE TABLES  IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT SELECT ON ALL VIEWS      IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT SELECT ON FUTURE VIEWS   IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;

-- ── 4) (OPTIONAL) Create the dbt user — uncomment only if HEALTHCARE_USER
--        does NOT already exist. Set your own password.
-- CREATE USER IF NOT EXISTS HEALTHCARE_USER
--   PASSWORD = 'ChangeThisStrongPassword!'
--   MUST_CHANGE_PASSWORD = TRUE;

-- ── 5) Attach the role to your dbt user + set sensible defaults ──────────────
GRANT ROLE HEALTHCARE_ROLE TO USER HEALTHCARE_USER;
ALTER USER HEALTHCARE_USER SET DEFAULT_ROLE = HEALTHCARE_ROLE;
ALTER USER HEALTHCARE_USER SET DEFAULT_WAREHOUSE = HEALTHCARE_WH;

LTER USER HEALTHCARE_USER SET DEFAULT_WAREHOUSE = HEALTHCARE_WH;


-- ── 5) Attach the role to YOUR user (ADMIN) ──────────────
GRANT ROLE HEALTHCARE_ROLE TO USER ADMIN;

-- ── 6) Verify ───────────────────────────────────────────────────────────────
SHOW SCHEMAS IN DATABASE HEALTHCARE_DB;
SHOW GRANTS TO ROLE HEALTHCARE_ROLE;