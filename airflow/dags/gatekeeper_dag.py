"""
gatekeeper_dag - Validate-before-load DQ pipeline.
Watches incoming/ folder, runs 12 tiered checks, loads or quarantines.
On a SUCCESSFUL load, triggers the main healthcare_pipeline so the new
data is transformed into Gold immediately (no waiting for the 10-min poll).

NOTE ON THE TWO GATEKEEPER EMAILS:
  1) QUARANTINE email  - sent by snowpark/gatekeeper.py when a FILE fails the
     12 checks. Detailed (file, failed checks, counts). Already logs to
     AUDIT.FILE_PROCESSING_LOG + AUDIT.DQ_METRICS_LOG.
  2) DAG-FAILURE email - sent by THIS file's on_failure_callback when the
     gatekeeper TASK ITSELF crashes (Snowflake unreachable, bad key, python
     error - not a quarantine). Formatted + logged to AUDIT.PIPELINE_ERROR_LOG,
     now including the ACTION line so the table row mirrors the email.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = "/opt/airflow/project"
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
    import datetime as _dt
    ti = context["task_instance"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    try_no = ti.try_number
    max_tries = ti.max_tries + 1
    run_type = str(context["dag_run"].run_type).split(".")[-1].lower()
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Plain-English meaning per task
    stage_meaning = {
        "gatekeeper_validate":
            "The gatekeeper worker itself failed to run (e.g. Snowflake unreachable, "
            "bad credentials, or a script error). This is NOT a file quarantine - "
            "if a file had failed the 12 checks you'd get a QUARANTINE email instead.",
        "trigger_healthcare_pipeline":
            "Failed to trigger the downstream healthcare_pipeline transform DAG.",
    }.get(task_id, "A gatekeeper task failed.")

    action_text = (f"Open Airflow > {dag_id} > {task_id} > Logs for the full error, "
                   f"fix the issue, then re-run.")

    subject = f"Gatekeeper Pipeline FAILED - {task_id}"
    body = (
        f"Healthcare GATEKEEPER pipeline FAILED - ingestion halted.\n\n"
        f"  DAG:            {dag_id}\n"
        f"  Failed task:    {task_id}\n"
        f"  Status:         FAILED (downstream steps skipped)\n"
        f"  Triggered by:   {run_type}\n"
        f"  Attempt:        {try_no} of {max_tries}\n"
        f"  Failed at:      {ts}\n\n"
        f"  What happened:\n"
        f"    - {stage_meaning}\n\n"
        f"  Action: {action_text}"
    )
    safe_subject = subject.replace("'", "")
    safe_body = body.replace("'", "")
    try:
        s = _snowflake_session()
        # 1) send the alert email
        s.sql("call system$send_email(?, ?, ?, ?)",
              params=["HEALTHCARE_EMAIL_INT", ALERT_EMAIL, safe_subject, safe_body]).collect()
        # 2) write the SAME details to the permanent error-log table (now incl. action)
        s.sql(
            "INSERT INTO HEALTHCARE_DB.AUDIT.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, triggered_by, attempt, error_summary, action, failed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            params=[dag_id, task_id, "FAILED", run_type,
                    f"{try_no} of {max_tries}", stage_meaning, action_text]).collect()
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
    dag_id="gatekeeper_pipeline",
    description="Validate-before-load gatekeeper for incoming files",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "data-quality", "gatekeeper"],
) as dag:

    gatekeeper = BashOperator(
        task_id="gatekeeper_validate",
        bash_command=f"cd {PROJECT} && python snowpark/gatekeeper.py",
    )

    # After a successful validate+load, trigger the main pipeline to
    # transform the new data into Gold immediately.
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_healthcare_pipeline",
        trigger_dag_id="healthcare_pipeline",
        wait_for_completion=False,   # fire-and-forget; main DAG runs independently
        reset_dag_run=True,
    )

    gatekeeper >> trigger_dbt