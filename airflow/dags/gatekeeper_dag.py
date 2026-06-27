"""
gatekeeper_dag — Validate-before-load DQ pipeline.
Watches incoming/ folder, runs 12 tiered checks, loads or quarantines.
Independent of the main Snowpipe→dbt pipeline.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
ALERT_EMAIL = "priyankapandey000111@gmail.com"


def send_failure_email(context):
    import subprocess, os
    task_id = context["task_instance"].task_id
    py = (
        "import os; from snowflake.snowpark import Session;"
        "from cryptography.hazmat.primitives import serialization;"
        "from cryptography.hazmat.backends import default_backend;"
        "k=open(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'],'rb').read();"
        "pk=serialization.load_pem_private_key(k,password=None,backend=default_backend());"
        "pkb=pk.private_bytes(encoding=serialization.Encoding.DER,"
        "format=serialization.PrivateFormat.PKCS8,"
        "encryption_algorithm=serialization.NoEncryption());"
        "s=Session.builder.configs({'account':os.environ['SNOWFLAKE_ACCOUNT'],"
        "'user':os.environ['SNOWFLAKE_USER'],'private_key':pkb,'role':'ACCOUNTADMIN',"
        "'warehouse':'HEALTHCARE_WH','database':'HEALTHCARE_DB','schema':'AUDIT'}).create();"
        f"s.sql(\"call system$send_email('HEALTHCARE_EMAIL_INT','{ALERT_EMAIL}',"
        f"'Gatekeeper DAG failed','Task {task_id} failed - check logs')\").collect(); s.close()"
    )
    try:
        subprocess.run(["python", "-c", py], check=True)
    except Exception as e:
        print(f"failure email could not be sent: {e}")


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

    gatekeeper