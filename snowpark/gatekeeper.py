"""
GATEKEEPER — Validate-BEFORE-Load Data Quality Pipeline (multi-feed, single path)
Handles patient_admissions, treatment_records, and insurance_claims.

This is the ONLY ingestion path. Snowpipe is retired. Every file — in any
batch (morning / afternoon / evening) — is uploaded to the incoming/ folder.

Per file:
  read the ACTUAL header  ->  run 12 tiered checks
    PASS  -> COPY INTO the real RAW table  ->  move file to processed/
    FAIL  -> move file to quarantine/  +  email  +  log, never loads
"""
import os
import uuid
import datetime
from dataclasses import dataclass
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# ── Shared settings (same for every feed) ──
COMMON = {
    "incoming_stage":    "HEALTHCARE_DB.RAW.GK_INCOMING",
    "processed_stage":   "HEALTHCARE_DB.RAW.GK_PROCESSED",
    "quarantine_stage":  "HEALTHCARE_DB.RAW.GK_QUARANTINE",
    "file_format":       "HEALTHCARE_DB.RAW.HEALTHCARE_CSV",
    "file_format_nohdr": "HEALTHCARE_DB.RAW.HEALTHCARE_CSV_NOHEADER",
    "email_integration": "HEALTHCARE_EMAIL_INT",
    "min_rows":          1,
    "max_null_pct":      5.0,
    "date_min":          "2020-01-01",
    "date_max":          "2030-12-31",
}

# ── Per-feed configs. The "file_pattern" decides which config a file uses. ──
# target_table now points at the REAL RAW tables (no more GK_ prefix), so the
# gatekeeper is the single validated front door to Bronze and dbt reads it directly.
CONFIGS = {
    "patient_admissions": {
        "file_pattern":     "patient_admissions",
        "target_table":     "HEALTHCARE_DB.RAW.PATIENT_ADMISSIONS",
        "expected_columns": ["admission_id", "patient_id", "doctor_id", "hospital_id",
                             "admit_date", "department", "admission_type",
                             "diagnosis_code", "length_of_stay", "readmission_flag"],
        "required_columns": ["admission_id", "patient_id", "doctor_id", "hospital_id"],
        "pk_column":        "admission_id",
        "numeric_columns":  ["admission_id", "length_of_stay"],
        "date_column":      "admit_date",
        "category_column":  "admission_type",
        "valid_categories": ["EMG", "URG", "ELC"],
        "load_columns":     "admission_id, patient_id, doctor_id, hospital_id, admit_date, "
                            "department, admission_type, diagnosis_code, length_of_stay, "
                            "readmission_flag, file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10",
    },
    "treatment_records": {
        "file_pattern":     "treatment_records",
        "target_table":     "HEALTHCARE_DB.RAW.TREATMENT_RECORDS",
        "expected_columns": ["treatment_id", "admission_id", "doctor_id", "procedure_code",
                             "treatment_date", "cost", "outcome"],
        "required_columns": ["treatment_id", "admission_id", "doctor_id"],
        "pk_column":        "treatment_id",
        "numeric_columns":  ["treatment_id", "admission_id", "cost"],
        "date_column":      "treatment_date",
        "category_column":  "outcome",
        "valid_categories": ["P", "F", "S"],
        "load_columns":     "treatment_id, admission_id, doctor_id, procedure_code, "
                            "treatment_date, cost, outcome, file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7",
    },
    "insurance_claims": {
        "file_pattern":     "insurance_claims",
        "target_table":     "HEALTHCARE_DB.RAW.INSURANCE_CLAIMS",
        "expected_columns": ["claim_id", "admission_id", "insurance_id", "claim_amount",
                             "approved_amount", "claim_status", "claim_date", "settle_date"],
        "required_columns": ["claim_id", "admission_id", "insurance_id"],
        "pk_column":        "claim_id",
        "numeric_columns":  ["claim_id", "admission_id", "claim_amount", "approved_amount"],
        "date_column":      "claim_date",
        "category_column":  "claim_status",
        "valid_categories": ["P", "A", "R"],
        "load_columns":     "claim_id, admission_id, insurance_id, claim_amount, "
                            "approved_amount, claim_status, claim_date, settle_date, "
                            "file_name, upload_dttm, load_dttm",
        "load_select":      "$1,$2,$3,$4,$5,$6,$7,$8",
    },
}


@dataclass
class DQResult:
    check_name: str
    tier: str
    passed: bool
    detail: str


def get_session():
    key = open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb").read()
    pk = serialization.load_pem_private_key(key, password=None, backend=default_backend())
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": pkb,
        "role": "ACCOUNTADMIN",
        "warehouse": "HEALTHCARE_WH",
        "database": "HEALTHCARE_DB",
        "schema": "RAW",
    }).create()


def detect_config(file_name):
    """Pick the right config based on which pattern appears in the filename."""
    for cfg in CONFIGS.values():
        if cfg["file_pattern"] in file_name.lower():
            return cfg
    return None


def list_incoming(session):
    rows = session.sql(f"LIST @{COMMON['incoming_stage']}").collect()
    files = []
    for r in rows:
        name = r["name"].split("/")[-1]
        if name.endswith(".csv") and detect_config(name) is not None:
            files.append(name)
    return files


def read_header(session, file_name, cfg):
    selects = ",".join([f"${i}" for i in range(1, len(cfg["expected_columns"]) + 2)])
    rows = session.sql(f"""
        SELECT {selects}
        FROM @{COMMON['incoming_stage']}/{file_name}
        (FILE_FORMAT => '{COMMON['file_format_nohdr']}')
        LIMIT 1
    """).collect()
    if not rows:
        return []
    return [str(v).strip().lower() for v in rows[0].as_dict().values()
            if v is not None and str(v).strip() != ""]


def load_to_temp(session, file_name, n_cols):
    cols_ddl = ", ".join([f"C{i} VARCHAR" for i in range(n_cols)])
    session.sql(f"CREATE OR REPLACE TEMP TABLE GK_TEMP_RAW ({cols_ddl})").collect()
    session.sql(f"""
        COPY INTO GK_TEMP_RAW
        FROM @{COMMON['incoming_stage']}/{file_name}
        FILE_FORMAT = (FORMAT_NAME = '{COMMON['file_format']}')
        ON_ERROR = CONTINUE
    """).collect()


def col_ref(header, name):
    name = name.lower()
    return f"C{header.index(name)}" if name in header else None


def run_checks(session, file_name, header, cfg):
    results = []
    R = results.append

    # ---- GATE (3) ----
    size_rows = session.sql(f"LIST @{COMMON['incoming_stage']}/{file_name}").collect()
    size = size_rows[0]["size"] if size_rows else 0
    R(DQResult("file_not_empty", "GATE", size > 0, f"size={size} bytes"))

    R(DQResult("column_count", "GATE",
               len(header) == len(cfg["expected_columns"]),
               f"found {len(header)}, expected {len(cfg['expected_columns'])}"))

    missing = [c for c in cfg["required_columns"] if c.lower() not in header]
    R(DQResult("required_columns", "GATE", len(missing) == 0,
               f"missing={missing}" if missing else "all present"))

    if any(r.tier == "GATE" and not r.passed for r in results):
        return results

    total = session.sql("SELECT COUNT(*) c FROM GK_TEMP_RAW").collect()[0]["C"]

    # ---- THRESHOLD (5) ----
    R(DQResult("row_count", "THRESHOLD", total >= COMMON["min_rows"], f"rows={total}"))

    null_details, null_ok = [], True
    for c in cfg["required_columns"]:
        ref = col_ref(header, c)
        n = session.sql(f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {ref} IS NULL").collect()[0]["C"]
        pct = (n / total * 100) if total else 0
        null_details.append(f"{c}={pct:.1f}%")
        if pct > COMMON["max_null_pct"]:
            null_ok = False
    R(DQResult("null_percentage", "THRESHOLD", null_ok, ", ".join(null_details)))

    num_checks = " OR ".join(
        [f"TRY_CAST({col_ref(header, c)} AS NUMBER) IS NULL" for c in cfg["numeric_columns"]])
    bad_num = session.sql(f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {num_checks}").collect()[0]["C"]
    R(DQResult("data_types", "THRESHOLD", bad_num == 0, f"non-numeric rows={bad_num}"))

    pk = col_ref(header, cfg["pk_column"])
    dupe_pk = session.sql(
        f"SELECT COUNT(*) c FROM (SELECT {pk} FROM GK_TEMP_RAW "
        f"GROUP BY {pk} HAVING COUNT(*) > 1)").collect()[0]["C"]
    R(DQResult("pk_uniqueness", "THRESHOLD", dupe_pk == 0, f"duplicate ids={dupe_pk}"))

    cat = col_ref(header, cfg["category_column"])
    bad_cat = session.sql(
        f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE {cat} NOT IN ("
        + ",".join([f"'{v}'" for v in cfg["valid_categories"]]) + ")").collect()[0]["C"]
    R(DQResult(f"valid_{cfg['category_column']}", "THRESHOLD", bad_cat == 0,
               f"invalid values={bad_cat}"))

    # ---- ADVISORY (2 generic) ----
    all_refs = ",".join([f"C{i}" for i in range(len(header))])
    dupe_rows = session.sql(
        "SELECT COUNT(*) c FROM (SELECT *, COUNT(*) OVER "
        f"(PARTITION BY {all_refs}) n FROM GK_TEMP_RAW) WHERE n > 1").collect()[0]["C"]
    R(DQResult("duplicate_rows", "ADVISORY", dupe_rows == 0, f"dup rows={dupe_rows}"))

    dref = col_ref(header, cfg["date_column"])
    bad_date = session.sql(
        f"SELECT COUNT(*) c FROM GK_TEMP_RAW WHERE TRY_TO_DATE({dref}) "
        f"NOT BETWEEN '{COMMON['date_min']}' AND '{COMMON['date_max']}'").collect()[0]["C"]
    R(DQResult("date_range", "ADVISORY", bad_date == 0, f"out-of-range dates={bad_date}"))

    return results


def load_file(session, file_name, cfg):
    """COPY the validated file into the REAL RAW table, filling the 3 metadata columns.
       Returns the number of rows this COPY added (not the whole-table count), so
       batch loads with repeated filenames report the correct per-batch row count."""
    before = session.sql(
        f"SELECT COUNT(*) c FROM {cfg['target_table']}").collect()[0]["C"]
    session.sql(f"""
        COPY INTO {cfg['target_table']}
        ({cfg['load_columns']})
        FROM (
            SELECT {cfg['load_select']},
                   METADATA$FILENAME,
                   METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ,
                   CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
            FROM @{COMMON['incoming_stage']}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = '{COMMON['file_format']}')
        ON_ERROR = ABORT_STATEMENT
    """).collect()
    after = session.sql(
        f"SELECT COUNT(*) c FROM {cfg['target_table']}").collect()[0]["C"]
    return after - before


def move_file(session, file_name, dest_stage):
    """Move the file out of incoming/ into a per-run TIMESTAMPED SUBFOLDER of the
    destination (processed/ or quarantine/). The timestamp subfolder means a client
    can upload the SAME filename every batch (morning/afternoon/evening) and nothing
    is ever overwritten — each batch keeps its own copy at
        processed/20260629_130245/patient_admissions.csv
    so the S3 trail matches the AUDIT.FILE_PROCESSING_LOG record."""
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    session.sql(f"COPY FILES INTO @{dest_stage}/{stamp}/ "
                f"FROM @{COMMON['incoming_stage']}/{file_name}").collect()
    session.sql(f"REMOVE @{COMMON['incoming_stage']}/{file_name}").collect()


def get_recipients(session):
    rows = session.sql(
        "SELECT recipient_email FROM HEALTHCARE_DB.AUDIT.EMAIL_RECIPIENT_LOG "
        "WHERE alert_type='DQ_FAILURE' AND is_active=TRUE").collect()
    return [r["RECIPIENT_EMAIL"] for r in rows]


def send_email(session, recipients, subject, body):
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    for to in recipients:
        session.sql("CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?)",
                    params=[COMMON["email_integration"], to, safe_subject, safe_body]).collect()


def log_file(session, run_id, file_name, status, rows, passed, failed):
    session.sql(
        "INSERT INTO HEALTHCARE_DB.AUDIT.FILE_PROCESSING_LOG "
        "VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
        params=[run_id, file_name, status, rows, passed, failed]).collect()


def log_checks(session, run_id, file_name, results):
    for r in results:
        session.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.DQ_METRICS_LOG "
            "VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[run_id, file_name, r.check_name, r.tier,
                    "PASS" if r.passed else "FAIL", r.detail]).collect()


def main():
    session = get_session()
    run_id = str(uuid.uuid4())[:8]
    print(f"[{run_id}] Gatekeeper starting")

    files = list_incoming(session)
    if not files:
        print(f"[{run_id}] No recognized files in incoming/ — nothing to do")
        session.close()
        return

    any_quarantined = False
    for file_name in files:
        cfg = detect_config(file_name)
        feed = cfg["file_pattern"]
        print(f"[{run_id}] Processing {file_name}  (feed: {feed})")

        header = read_header(session, file_name, cfg)
        load_to_temp(session, file_name, max(len(header), 1))
        results = run_checks(session, file_name, header, cfg)
        log_checks(session, run_id, file_name, results)

        gate_fail = [r for r in results if r.tier == "GATE" and not r.passed]
        thresh_fail = [r for r in results if r.tier == "THRESHOLD" and not r.passed]
        passed_n = sum(1 for r in results if r.passed)
        failed_n = sum(1 for r in results if not r.passed)

        if gate_fail or thresh_fail:
            any_quarantined = True
            move_file(session, file_name, COMMON["quarantine_stage"])
            log_file(session, run_id, file_name, "QUARANTINED", 0, passed_n, failed_n)
            failed_block = "\n".join([f"    - {r.check_name} [{r.tier}]: {r.detail}"
                                      for r in (gate_fail + thresh_fail)])
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            email_body = (
                f"Healthcare GATEKEEPER validation FAILED - file quarantined.\n\n"
                f"  File:           {file_name}\n"
                f"  Feed:           {feed}\n"
                f"  Status:         QUARANTINED (not loaded)\n"
                f"  Checks passed:  {passed_n}\n"
                f"  Checks failed:  {failed_n}\n"
                f"  Quarantined at: {ts}\n\n"
                f"  Failed checks:\n{failed_block}\n\n"
                f"  Action: File moved to quarantine/ folder. Review and re-upload a corrected file."
            )
            send_email(session, get_recipients(session),
                       f"GATEKEEPER QUARANTINE - {file_name}", email_body)
            print(f"[{run_id}] QUARANTINED {file_name}")
        else:
            rows = load_file(session, file_name, cfg)
            move_file(session, file_name, COMMON["processed_stage"])
            log_file(session, run_id, file_name, "PASSED", rows, passed_n, failed_n)
            advisory_warn = [r for r in results if r.tier == "ADVISORY" and not r.passed]
            warn = (" (advisories: " + ", ".join(r.check_name for r in advisory_warn) + ")"
                    if advisory_warn else "")
            print(f"[{run_id}] PASSED {file_name}: loaded {rows} rows{warn}")

    session.close()
    if any_quarantined:
        raise RuntimeError(f"[{run_id}] Gatekeeper: one or more files quarantined.")
    print(f"[{run_id}] Gatekeeper complete — all files passed")


if __name__ == "__main__":
    main()