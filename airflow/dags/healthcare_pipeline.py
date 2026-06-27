"""
healthcare_pipeline DAG
Sensor-driven: checks for new files every 10 min, runs full pipeline only when found.
quality check → dbt run → dbt test → reconciliation. Emails on ANY failure.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sdk import task   # modern Airflow 3 task decorator

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
    task_id = context["task_instance"].task_id
    exec_dt = context["ts"]
    subject = f"Airflow Pipeline FAILED - {task_id}"
    body = f"Task {task_id} failed at {exec_dt}. Check Airflow logs."
    try:
        s = _snowflake_session()
        s.sql(f"call system$send_email('HEALTHCARE_EMAIL_INT','{ALERT_EMAIL}',"
              f"'{subject}','{body}')").collect()
        s.close()
    except Exception as e:
        print(f"failure email could not be sent: {e}")


default_args = {
    "owner": "priyanka",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_email,
}

with DAG(
    dag_id="healthcare_pipeline",
    description="Sensor-driven healthcare pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=10),   # wake up every 10 min to check
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "dbt", "snowflake"],
) as dag:

    # ── SENSOR: only proceed if new files arrived in the last 15 min ──
    @task.sensor(poke_interval=60, timeout=300, mode="reschedule")
    def wait_for_new_files():
        s = _snowflake_session()
        try:
            rows = s.sql("""
                select count(*) as new_files from (
                    select file_name from table(information_schema.copy_history(
                        table_name=>'HEALTHCARE_DB.RAW.PATIENT_ADMISSIONS',
                        start_time=>dateadd(minute,-15,current_timestamp())))
                    union all
                    select file_name from table(information_schema.copy_history(
                        table_name=>'HEALTHCARE_DB.RAW.TREATMENT_RECORDS',
                        start_time=>dateadd(minute,-15,current_timestamp())))
                    union all
                    select file_name from table(information_schema.copy_history(
                        table_name=>'HEALTHCARE_DB.RAW.INSURANCE_CLAIMS',
                        start_time=>dateadd(minute,-15,current_timestamp())))
                )
            """).collect()
            new_count = rows[0]["NEW_FILES"]
            print(f"New files detected in last 15 min: {new_count}")
            return new_count > 0       # True = proceed, False = keep waiting
        finally:
            s.close()

    sensor = wait_for_new_files()

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"cd {PROJECT} && python snowpark/quality_check.py",
    )
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
        bash_command=f"cd {PROJECT} && echo 'Pipeline complete — see AUDIT.FILE_RECONCILIATION'",
    )

    sensor >> quality_check >> dbt_run >> dbt_snapshot >> dbt_test >> reconciliation               