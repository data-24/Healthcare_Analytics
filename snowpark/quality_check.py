"""
quality_check.py — Bronze gatekeeper with column-level detail.
Reads each file's header, compares to the expected schema, and on mismatch
logs to AUDIT.DATA_QUALITY_ERRORS + emails the exact reason (which column).
Key-pair auth, config from env vars — no hardcoded secrets.
"""
import os, uuid, datetime
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ── expected schema per table (single source of truth) ──────────────
EXPECTED = {
    "PATIENT_ADMISSIONS": ["admission_id","patient_id","doctor_id","hospital_id",
        "admit_date","department","admission_type","diagnosis_code",
        "length_of_stay","readmission_flag"],
    "TREATMENT_RECORDS": ["treatment_id","admission_id","doctor_id","procedure_code",
        "treatment_date","cost","outcome"],
    "INSURANCE_CLAIMS": ["claim_id","admission_id","insurance_id","claim_amount",
        "approved_amount","claim_status","claim_date","settle_date"],
}
# which uploaded filename maps to which table
FILE_MAP = {
    "patient_admissions": "PATIENT_ADMISSIONS",
    "treatment_records":  "TREATMENT_RECORDS",
    "insurance_claims":   "INSURANCE_CLAIMS",
}
STAGE = "HEALTHCARE_DB.RAW.HEALTHCARE_STAGE"
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


def log_error(session, run_id, table, file_name, error):
    session.sql(f"""
        INSERT INTO HEALTHCARE_DB.AUDIT.DATA_QUALITY_ERRORS
            (run_id, table_name, file_name, error_description, rows_rejected, failed_at)
        VALUES ('{sq(run_id)}','{sq(table)}','{sq(file_name)}','{sq(error)}',0,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
    """).collect()


def send_email(session, file_name, error, failed_at):
    body = (f"Healthcare pipeline quality check FAILED.\n\n"
            f"  File:      {file_name}\n"
            f"  Error:     {error}\n"
            f"  Failed at: {failed_at}\n\n"
            f"The file did NOT pass validation. Please fix and re-upload.")
    session.sql(f"""
        CALL SYSTEM$SEND_EMAIL('HEALTHCARE_EMAIL_INT','{sq(ALERT_EMAIL)}',
            'Healthcare Pipeline FAILED - {sq(file_name)}','{sq(body)}')
    """).collect()


def main():
    run_id = str(uuid.uuid4())[:8]
    session = get_session()
    failures = 0
    try:
        files = list_files(session)
        print(f"[{run_id}] Files in stage: {files}")

        for file_name in files:
            # figure out which table this file is for
            table = next((t for k, t in FILE_MAP.items() if k in file_name.lower()), None)
            if not table:
                print(f"  SKIP {file_name} — no matching table")
                continue

            actual = read_header(session, file_name)
            expected = EXPECTED[table]
            ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            if actual == expected:
                print(f"  PASS {file_name} — {len(actual)} columns OK")
                continue

            # mismatch → build a precise reason
            missing = [c for c in expected if c not in actual]
            extra   = [c for c in actual if c not in expected]
            error = (f"Column mismatch: expected {len(expected)} columns, found {len(actual)}. "
                     f"Missing: {missing or 'none'}. Extra: {extra or 'none'}.")
            print(f"  FAIL {file_name} — {error}")
            log_error(session, run_id, table, file_name, error)
            send_email(session, file_name, error, ts)
            failures += 1

        if failures:
            raise RuntimeError(f"[{run_id}] Quality check FAILED: {failures} file(s). Emails sent.")
        print(f"[{run_id}] Quality check PASSED — all files valid.")
    finally:
        session.close()


if __name__ == "__main__":
    main()