from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# --- Paths ---
PROJECT = "/mnt/d/Github/canadian-grocery-price-analytics"
DBT_DIR = f"{PROJECT}/dbt"
PY_BIN = "/home/linko/pipeline-venv/bin/python"
DBT_BIN = "/home/linko/pipeline-venv/bin/dbt"

def dbt_cmd(dbt_args: str) -> str:
    # --- Load .env -> point dbt to in-project profiles -> run dbt by absolute path ---
    return (
        f"cd {DBT_DIR} && "
        f"set -a && . {PROJECT}/.env && set +a && "
        f"export DBT_PROFILES_DIR={DBT_DIR} && "
        f"{DBT_BIN} {dbt_args}"
    )

with DAG(
    dag_id="grocery_pipeline",
    description="Ingest Project Hammer data, build the star schema, test it",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["grocery", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"cd {PROJECT} && "
            f"set -a && . {PROJECT}/.env && set +a && "
            f"{PY_BIN} ingestion/load_to_snowflake.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd("run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("test"),
    )

    ingest >> dbt_run >> dbt_test