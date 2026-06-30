"""
quality_check.py — OPTIONAL pre-flight schema checker for the incoming/ folder.

NOTE ON ITS ROLE (read this):
    Snowpipe is retired. The GATEKEEPER (gatekeeper.py) is now the single, real
    ingestion gate — its GATE tier already checks column count + required columns
    on every file BEFORE loading, and quarantines anything that fails.

    This script is therefore NO LONGER part of the load path. It is kept only as a
    convenience: a quick, read-only "are the headers right?" scan of incoming/ that
    you can run by hand before kicking off the gatekeeper, so you can spot an
    obviously wrong file early. It LOADS NOTHING and moves nothing.

    If you don't want it, you can delete this file — the gatekeeper covers everything
    it does. If you keep it, remove the old 'quality_check' + 'check_snowpipe_errors'
    tasks from the main Airflow DAG (healthcare_pipeline.py); they belonged to the
    retired Snowpipe path.

Key-pair auth, config from env vars — no hardcoded secrets.
"""
import os
import uuid
import datetime
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ── expected schema per table (single source of truth) ──────────────
EXPECTED = {
    "PATIENT_ADMISSIONS": ["admission_id", "patient_id", "doctor_id", "hospital_id",
        "admit_date", "department", "admission_type", "diagnosis_code",
        "length_of_stay", "readmission_flag"],
    "TREATMENT_RECORDS": ["treatment_id", "admission_id", "doctor_id", "procedure_code",
        "treatment_date", "cost", "outcome"],
    "INSURANCE_CLAIMS": ["claim_id", "admission_id", "insurance_id", "claim_amount",
        "approved_amount", "claim_status", "claim_date", "settle_date"],
}
# which uploaded filename keyword maps to which table
FILE_MAP = {
    "patient_admissions": "PATIENT_ADMISSIONS",
    "treatment_records":  "TREATMENT_RECORDS",
    "insurance_claims":   "INSURANCE_CLAIMS",
}

# Snowpipe is retired — this now scans the GATEKEEPER incoming/ folder.
STAGE = "HEALTHCARE_DB.RAW.GK_INCOMING"
NOHEADER_FMT = "HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER"
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "your_email@example.com")


def get_session() -> Session:
    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": pkb,
        "role": "ACCOUNTADMIN", "warehouse": "HEALTHCARE_WH",
        "database": "HEALTHCARE_DB", "schema": "RAW",
    }).create()


def sq(s):
    return str(s or "").replace("'", "''")


def list_files(session):
    rows = session.sql(f"LIST @{STAGE}").collect()
    return [r["name"].split("/")[-1] for r in rows if r["name"].lower().endswith(".csv")]


def read_header(session, file_name):
    """Read the first (header) row of a stage file as a list of column names."""
    rows = session.sql(f"""
        SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
        FROM @{STAGE}/{file_name} (FILE_FORMAT => '{NOHEADER_FMT}')
        LIMIT 1
    """).collect()
    if not rows:
        return []
    row = rows[0]
    return [str(row[i]).strip().lower() for i in range(12) if row[i] is not None]


def main():
    run_id = str(uuid.uuid4())[:8]
    session = get_session()
    problems = 0
    try:
        files = list_files(session)
        if not files:
            print(f"[{run_id}] incoming/ is empty — nothing to pre-check.")
            return
        print(f"[{run_id}] Pre-flight schema check on incoming/: {files}")

        for file_name in files:
            table = next((t for k, t in FILE_MAP.items() if k in file_name.lower()), None)
            if not table:
                print(f"  SKIP {file_name} — filename has no table keyword "
                      f"(patient_admissions / treatment_records / insurance_claims)")
                continue

            actual = read_header(session, file_name)
            expected = EXPECTED[table]

            if actual == expected:
                print(f"  OK   {file_name} — {len(actual)} columns match {table}")
                continue

            missing = [c for c in expected if c not in actual]
            extra = [c for c in actual if c not in expected]
            print(f"  WARN {file_name} -> {table}: expected {len(expected)} cols, "
                  f"found {len(actual)}. Missing: {missing or 'none'}. Extra: {extra or 'none'}.")
            problems += 1

        if problems:
            print(f"[{run_id}] Pre-flight found {problems} file(s) with schema issues. "
                  f"The gatekeeper will quarantine these when it runs.")
        else:
            print(f"[{run_id}] Pre-flight PASSED — all incoming files have the right headers.")
    finally:
        session.close()


if __name__ == "__main__":
    main()