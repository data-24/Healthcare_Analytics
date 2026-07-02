"""
healthcare_pipeline DAG  (trigger-only, no sensor)
Transforms validated Bronze data into Silver + Gold.
  dbt run -> dbt snapshot -> dbt test -> reconciliation.

The GATEKEEPER (gatekeeper_dag.py) is the single validated ingestion path: it
validates every file, loads good ones into RAW, quarantines bad ones, then
TRIGGERS this pipeline. This DAG runs when the gatekeeper triggers it (and on a
10-min backup schedule).

ALERTS (email only on REAL breakage):
  - dbt run breaks         -> dbt_run task fails  -> email names the FAILING MODEL
  - any dbt test fails      -> dbt_test task fails -> email
  - any task error          -> on_failure_callback -> email + PIPELINE_ERROR_LOG

ROOT-CAUSE CAPTURE:
  Each dbt task writes its output to a log file under /tmp. When a task fails,
  the callback reads that file, finds the actual dbt ERROR line (e.g. the failing
  model name like STAGING.stg_admissions), and puts it in BOTH the email and the
  PIPELINE_ERROR_LOG row -- so you see the real cause, not a generic message.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
PROFILES_DIR = "/opt/airflow/config"
ALERT_EMAIL = "priyankapandey000111@gmail.com"

# Each task tees its output here so the failure callback can read the real error.
LOG_DIR = "/tmp/dbt_task_logs"


def _snowflake_session():
    import os
    from snowflake.snowpark import Session
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    k = open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb").read()
    pk = serialization.load_pem_private_key(k, password=None, backend=default_backend())
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"],
        "private_key": pkb, "role": "ACCOUNTADMIN", "warehouse": "HEALTHCARE_WH",
        "database": "HEALTHCARE_DB", "schema": "AUDIT"}).create()


def extract_dbt_error(task_id):
    """Read the task's dbt output log and pull out the real error line(s).
       Returns a short, specific string like:
         'Model failed to build: STAGING.stg_admissions'
       Falls back to a generic message if no log / no ERROR line is found."""
    import os, re
    log_path = os.path.join(LOG_DIR, f"{task_id}.log")
    generic = {
        "dbt_run":        "dbt failed to build the Silver/Gold models (broken SQL or model error).",
        "dbt_snapshot":   "dbt failed to capture SCD2 snapshot history.",
        "dbt_test":       "One or more dbt data-quality tests failed.",
        "reconciliation": "The reconciliation step failed.",
    }.get(task_id, "A pipeline task failed.")

    if not os.path.exists(log_path):
        return generic

    try:
        with open(log_path, "r", errors="ignore") as f:
            text = f.read()
    except Exception:
        return generic

    # Strip ANSI colour codes dbt writes to the console (e.g. \x1b[31m)
    clean = re.sub(r"\x1b\[[0-9;]*m", "", text)
    lines = clean.splitlines()

    hits = []

    # 1) Model build failures:  "ERROR creating sql ... model STAGING.stg_admissions"
    for ln in lines:
        m = re.search(r"ERROR creating.*model\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Model failed to build: " + m.group(1))

    # 2) Compilation errors:  "Compilation Error in model stg_admissions (path)"
    for ln in lines:
        m = re.search(r"Compilation Error in (model|test|snapshot)\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Compilation error in " + m.group(1) + ": " + m.group(2))

    # 3) Database errors:  "Database Error in model stg_admissions"
    for ln in lines:
        m = re.search(r"Database Error in (model|test|snapshot)\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Database error in " + m.group(1) + ": " + m.group(2))

    # 4) Failing tests:  "Failure in test not_null_fct_claims_insurance_sk"
    for ln in lines:
        m = re.search(r"Failure in test\s+([A-Za-z0-9_.]+)", ln)
        if m:
            hits.append("Test failed: " + m.group(1))

    if not hits:
        return generic

    # De-duplicate while preserving order, cap length so the email stays tidy.
    seen, unique = set(), []
    for h in hits:
        if h not in seen:
            seen.add(h)
            unique.append(h)
    return "  ".join(unique[:5])


def send_failure_email(context):
    import datetime
    ti = context["task_instance"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    try_no = ti.try_number
    max_tries = ti.max_tries + 1
    run_type = str(context["dag_run"].run_type).split(".")[-1].lower()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Pull the REAL failing model/test out of the task's dbt log:
    root_cause = extract_dbt_error(task_id)

    action_text = ("Open Airflow > " + dag_id + " > " + task_id + " > Logs for the full "
                   "stack trace, fix the issue, then re-run the pipeline.")

    subject = "Airflow Pipeline FAILED - " + task_id
    body = (
        "Healthcare AIRFLOW pipeline FAILED - transform halted.\n\n"
        "  DAG:            " + dag_id + "\n"
        "  Failed task:    " + task_id + "\n"
        "  Status:         FAILED (downstream steps skipped)\n"
        "  Triggered by:   " + run_type + "\n"
        "  Attempt:        " + str(try_no) + " of " + str(max_tries) + "\n"
        "  Failed at:      " + ts + "\n\n"
        "  Root cause:\n"
        "    - " + root_cause + "\n\n"
        "  Action: " + action_text
    )
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    try:
        s = _snowflake_session()
        s.sql("call system$send_email(?, ?, ?, ?)",
              params=["HEALTHCARE_EMAIL_INT", ALERT_EMAIL, safe_subject, safe_body]).collect()
        # Table row now mirrors the email: root cause in error_summary + the action line.
        s.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, triggered_by, attempt, error_summary, action, failed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[dag_id, task_id, "FAILED", run_type,
                    str(try_no) + " of " + str(max_tries), root_cause, action_text]).collect()
        s.close()
    except Exception as e:
        print("failure email/log could not be written: " + str(e))


default_args = {
    "owner": "priyanka",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_email,
}


# Each dbt command pipes its output through `tee` into a per-task log file that
# the failure callback reads. `set -o pipefail` ensures the task still FAILS when
# dbt fails (tee would otherwise mask the exit code). `2>&1` captures errors too.
def dbt_cmd(task_id, dbt_sub):
    return (
        "mkdir -p " + LOG_DIR + " && set -o pipefail && "
        "cd " + PROJECT + " && "
        "dbt " + dbt_sub + " --profiles-dir " + PROFILES_DIR + " --project-dir " + PROJECT + " "
        "2>&1 | tee " + LOG_DIR + "/" + task_id + ".log"
    )


with DAG(
    dag_id="healthcare_pipeline",
    description="Trigger-only healthcare transform pipeline (single gatekeeper path)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "dbt", "snowflake"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd("dbt_run", "run"),
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=dbt_cmd("dbt_snapshot", "snapshot"),
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("dbt_test", "test"),
    )
    reconciliation = BashOperator(
        task_id="reconciliation",
        bash_command="cd " + PROJECT + " && echo 'Pipeline complete - see AUDIT logs'",
    )

    dbt_run >> dbt_snapshot >> dbt_test >> reconciliation