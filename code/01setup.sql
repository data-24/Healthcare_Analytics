-- ============================================================================
-- SCRIPT 01 — ACCOUNT SETUP
-- Warehouse + Database + Schemas + Role + Grants
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   Snowflake is organised as:  Warehouse (compute) + Database > Schema > Table.
--   This script creates ALL the containers the project lives in:
--     • 1 warehouse  (the "engine" that runs queries — you pay only when it runs)
--     • 1 database   (HEALTHCARE_DB — the top-level folder)
--     • 5 schemas    (RAW, STAGING, MARTS, SNAPSHOTS, AUDIT — sub-folders)
--     • 1 role       (HEALTHCARE_ROLE — a "job badge" that owns the objects)
--
-- RUN THIS FIRST. Run the whole file top to bottom in a Snowsight worksheet.
-- ============================================================================

-- Use the highest-level admin role to create account objects.
USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1) WAREHOUSE — the compute engine
--    XSMALL is the cheapest. AUTO_SUSPEND=60 means it powers off after 60s idle
--    so you stop paying. AUTO_RESUME wakes it automatically on the next query.
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Compute engine for the healthcare analytics pipeline';

-- ----------------------------------------------------------------------------
-- 2) DATABASE — the top-level folder for everything
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS HEALTHCARE_DB
  COMMENT = 'Healthcare patient journey & revenue analytics';

-- ----------------------------------------------------------------------------
-- 3) SCHEMAS — the medallion layers (sub-folders inside the database)
--    RAW       = Bronze : exact copies of source files
--    STAGING   = Silver : cleaned + typed + decoded data (dbt builds this)
--    MARTS     = Gold   : star schema dims + facts (dbt builds this)
--    SNAPSHOTS =          slowly-changing history (dbt builds this)
--    AUDIT     =          monitoring/logging tables (we hand-build these)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.RAW       COMMENT = 'Bronze: raw landed data';
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.STAGING   COMMENT = 'Silver: cleaned and typed';
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.MARTS     COMMENT = 'Gold: star schema for BI';
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.SNAPSHOTS COMMENT = 'SCD2 history';
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.AUDIT     COMMENT = 'Pipeline monitoring & logs';

-- ----------------------------------------------------------------------------
-- 4) ROLE — the "job badge" that will own and operate the project
--    Using a dedicated role (instead of ACCOUNTADMIN) is best practice:
--    it only gets the privileges it actually needs.
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS HEALTHCARE_ROLE
  COMMENT = 'Operating role for dbt + pipeline';

-- Let the role use the engine and see the database + all its schemas.
GRANT USAGE ON WAREHOUSE HEALTHCARE_WH         TO ROLE HEALTHCARE_ROLE;
GRANT USAGE ON DATABASE  HEALTHCARE_DB         TO ROLE HEALTHCARE_ROLE;
GRANT USAGE ON ALL SCHEMAS    IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE HEALTHCARE_DB TO ROLE HEALTHCARE_ROLE;

-- Let the role CREATE tables/views in the layers dbt and the pipeline write to.
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_DB.RAW       TO ROLE HEALTHCARE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_DB.STAGING   TO ROLE HEALTHCARE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_DB.MARTS     TO ROLE HEALTHCARE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_DB.SNAPSHOTS TO ROLE HEALTHCARE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HEALTHCARE_DB.AUDIT     TO ROLE HEALTHCARE_ROLE;

-- Let the role read + write the RAW + AUDIT tables (pipeline loads & logs go here).
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA HEALTHCARE_DB.RAW   TO ROLE HEALTHCARE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA HEALTHCARE_DB.RAW   TO ROLE HEALTHCARE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA HEALTHCARE_DB.AUDIT TO ROLE HEALTHCARE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA HEALTHCARE_DB.AUDIT TO ROLE HEALTHCARE_ROLE;

-- ----------------------------------------------------------------------------
-- 5) ATTACH THE ROLE TO YOUR LOGIN so you can actually use it.
--    Replace ADMIN with your Snowflake username if different.
-- ----------------------------------------------------------------------------
GRANT ROLE HEALTHCARE_ROLE TO USER ADMIN;

-- Done. Next: run 02_storage_integration.sql