-- ============================================================================
-- SCRIPT 02 — AWS S3 STORAGE INTEGRATION  (secure, key-less link to S3)
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (in plain English):
--   A "storage integration" is a secure handshake that lets Snowflake read your
--   S3 bucket WITHOUT ever putting AWS keys/passwords in SQL. Instead, Snowflake
--   "assumes" an AWS IAM role you create. This is the safe, industry-standard way.
--
-- BEFORE RUNNING — do the AWS side first (see comments at bottom of this file):
--   1. Create an S3 bucket + folders (incoming/, processed/, quarantine/)
--   2. Create an IAM policy + role that allows reading that bucket
--   3. Copy the IAM role's ARN into the placeholder below
--
-- AFTER RUNNING — you finish the handshake:
--   Run DESC INTEGRATION (bottom), copy 2 values it returns back into the
--   AWS role's "trust relationship". Full instructions at the bottom.
--
-- ⚠️ REPLACE THE PLACEHOLDERS:
--     <YOUR_AWS_ACCOUNT_ID>  → your 12-digit AWS account number
--     <YOUR_BUCKET>          → your S3 bucket name
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Create the integration. It points at ONE IAM role and ALLOWS only the
-- folders you list. Nothing else in your AWS account is reachable.
-- ----------------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS HEALTHCARE_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-healthcare-role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://<YOUR_BUCKET>/incoming/',
    's3://<YOUR_BUCKET>/processed/',
    's3://<YOUR_BUCKET>/quarantine/'
  )
  COMMENT = 'Secure key-less link to the healthcare S3 bucket';

-- Let the operating role use this integration (so stages can be built on it).
GRANT USAGE ON INTEGRATION HEALTHCARE_S3_INT TO ROLE HEALTHCARE_ROLE;

-- ----------------------------------------------------------------------------
-- FINISH THE HANDSHAKE — run this, then copy 2 values into AWS.
-- From the output, copy:
--    STORAGE_AWS_IAM_USER_ARN   → paste into AWS role Trust policy "Principal.AWS"
--    STORAGE_AWS_EXTERNAL_ID    → paste into AWS role Trust policy "sts:ExternalId"
-- ----------------------------------------------------------------------------
DESC INTEGRATION HEALTHCARE_S3_INT;

-- ============================================================================
-- AWS SIDE — DO THIS BEFORE/AROUND THE ABOVE (one time, in the AWS console)
-- ----------------------------------------------------------------------------
-- A) CREATE THE BUCKET
--    S3 → Create bucket → name = <YOUR_BUCKET>, your region (e.g. eu-west-2).
--    Keep "Block all public access" ON. Create 3 folders inside:
--        incoming/     (new files land here)
--        processed/    (gatekeeper moves good files here)
--        quarantine/   (gatekeeper moves bad files here)
--
-- B) CREATE AN IAM POLICY  (IAM → Policies → Create → JSON), replace <YOUR_BUCKET>:
--    {
--      "Version": "2012-10-17",
--      "Statement": [
--        { "Effect": "Allow",
--          "Action": ["s3:GetObject","s3:GetObjectVersion","s3:PutObject","s3:DeleteObject"],
--          "Resource": "arn:aws:s3:::<YOUR_BUCKET>/*" },
--        { "Effect": "Allow",
--          "Action": ["s3:ListBucket","s3:GetBucketLocation"],
--          "Resource": "arn:aws:s3:::<YOUR_BUCKET>" }
--      ]
--    }
--    Name it:  snowflake-healthcare-policy
--
-- C) CREATE AN IAM ROLE  (IAM → Roles → Create → "AWS account" → This account),
--    attach the policy above, name it:  snowflake-healthcare-role
--    Copy its Role ARN into STORAGE_AWS_ROLE_ARN at the top of this script.
--
-- D) AFTER running DESC INTEGRATION above, edit the role's "Trust relationships":
--    {
--      "Version": "2012-10-17",
--      "Statement": [{
--        "Effect": "Allow",
--        "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN from Snowflake>" },
--        "Action": "sts:AssumeRole",
--        "Condition": { "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Snowflake>" } }
--      }]
--    }
--
-- Next: run 03_file_formats_and_stages.sql
-- ============================================================================