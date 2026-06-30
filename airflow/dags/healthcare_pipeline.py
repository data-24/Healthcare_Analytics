"""
healthcare_pipeline DAG  (trigger-only, no sensor)
Transforms validated Bronze data into Silver + Gold.
  dbt run -> dbt snapshot -> dbt test -> reconciliation.

Snowpipe is retired. The GATEKEEPER (gatekeeper_dag.py) is the single validated
ingestion path: it validates every file, loads good ones into RAW, quarantines bad
ones, then TRIGGERS this pipeline. So this DAG no longer polls for new files with a
sensor — it runs when the gatekeeper triggers it (and on its 10-min schedule as a
backup).

ALERTS (email only on REAL breakage):
  - schema mismatch / any of the 12 checks fail  -> emailed by the GATEKEEPER
  - dbt run breaks                               -> dbt_run task fails  -> email
  - any of the 41 dbt tests fail                 -> dbt_test task fails  -> email
  - any task error                               -> on_failure_callback -> email
There is no data-watching sensor, so there are no false-alarm "no new files" emails.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
PROFILES_DIR = "/opt/airflow/config"
ALERT_EMAIL = "priyankapandey000111@gmail.com"


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


def send_failure_email(context):
    import datetime
    ti = context["task_instance"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    try_no = ti.try_number
    max_tries = ti.max_tries + 1
    # Clean label: 'DagRunType.MANUAL' -> 'manual', 'DagRunType.SCHEDULED' -> 'scheduled'
    run_type = str(context["dag_run"].run_type).split(".")[-1].lower()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Plain-English description of what each task failing means
    stage_meaning = {
        "dbt_run":      "dbt failed to build the Silver/Gold models (broken SQL or model error).",
        "dbt_snapshot": "dbt failed to capture SCD2 snapshot history.",
        "dbt_test":     "One or more of the 36 dbt data-quality tests failed.",
        "reconciliation": "The reconciliation step failed.",
    }.get(task_id, "A pipeline task failed.")

    subject = f"Airflow Pipeline FAILED - {task_id}"
    body = (
        f"Healthcare AIRFLOW pipeline FAILED - transform halted.\n\n"
        f"  DAG:            {dag_id}\n"
        f"  Failed task:    {task_id}\n"
        f"  Status:         FAILED (downstream steps skipped)\n"
        f"  Triggered by:   {run_type}\n"
        f"  Attempt:        {try_no} of {max_tries}\n"
        f"  Failed at:      {ts}\n\n"
        f"  What happened:\n"
        f"    - {stage_meaning}\n\n"
        f"  Action: Open Airflow > {dag_id} > {task_id} > Logs for the full error, "
        f"fix the issue, then re-run the pipeline."
    )
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    try:
        s = _snowflake_session()
        # 1) send the alert email
        s.sql("call system$send_email(?, ?, ?, ?)",
              params=["HEALTHCARE_EMAIL_INT", ALERT_EMAIL, safe_subject, safe_body]).collect()
        # 2) write the SAME details to the permanent error-log table
        s.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, triggered_by, attempt, error_summary, failed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[dag_id, task_id, "FAILED", run_type,
                    f"{try_no} of {max_tries}", stage_meaning]).collect()
        s.close()
    except Exception as e:
        print(f"failure email/log could not be written: {e}")


default_args = {
    "owner": "priyanka",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_email,
}

with DAG(
    dag_id="healthcare_pipeline",
    description="Trigger-only healthcare transform pipeline (single gatekeeper path)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),   # backup schedule; normally triggered by gatekeeper
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "dbt", "snowflake"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT} && dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT}",
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {PROJECT} && dbt snapshot --profiles-dir {PROFILES_DIR} --project-dir {PROJECT}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT} && dbt test --profiles-dir {PROFILES_DIR} --project-dir {PROJECT}",
    )
    reconciliation = BashOperator(
        task_id="reconciliation",
        bash_command=f"cd {PROJECT} && echo 'Pipeline complete - see AUDIT.FILE_RECONCILIATION'",
    )

    dbt_run >> dbt_snapshot >> dbt_test >> reconciliation