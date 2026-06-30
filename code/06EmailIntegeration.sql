-- ============================================================================
-- SCRIPT 06 — EMAIL NOTIFICATION INTEGRATION
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   Lets Snowflake send alert emails (e.g. "a file was quarantined") using the
--   built-in function SYSTEM$SEND_EMAIL. You create a notification integration
--   listing the allowed recipient addresses, then any code can send to them.
--
--   Emails arrive FROM no-reply@snowflake.net. The SUBJECT line says what the
--   alert is about. This is the same channel the gatekeeper + DAGs use.
--
-- ⚠️ REPLACE <YOUR_ALERT_EMAIL> with the same inbox you used in script 05.
--    Every address that might receive mail MUST be listed in ALLOWED_RECIPIENTS.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Create the email integration (the "allow-list" of who can be emailed).
-- ----------------------------------------------------------------------------
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS HEALTHCARE_EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('<YOUR_ALERT_EMAIL>')
  COMMENT = 'Sends pipeline alert emails';

-- Let the operating role use it (so the gatekeeper/DAGs can send).
GRANT USAGE ON INTEGRATION HEALTHCARE_EMAIL_INT TO ROLE HEALTHCARE_ROLE;

-- ----------------------------------------------------------------------------
-- TEST — send yourself a test email to confirm it works.
-- The 4 arguments are: (integration_name, recipients, subject, body).
-- ----------------------------------------------------------------------------
CALL SYSTEM$SEND_EMAIL(
  'HEALTHCARE_EMAIL_INT',
  '<YOUR_ALERT_EMAIL>',
  'Healthcare pipeline — email integration test',
  'If you can read this, Snowflake email alerts are working.'
);

-- Next: run 07_load_seeds_note.sql