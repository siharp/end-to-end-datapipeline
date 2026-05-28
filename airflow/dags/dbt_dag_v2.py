import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt/olist_warehouse"

default_args = {
    "owner": "data-platform",
    "retries": 1,
}

with DAG(
    dag_id="dbt_olist_production",
    description="Production dbt pipeline (clean & scalable)",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "production"],
) as dag:

    # -------------------------
    # 2. COMPILE (optional)
    # -------------------------
    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt compile --profiles-dir .
        """
    )

    # -------------------------
    # 3. STAGING LAYER
    # -------------------------
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt build --select staging+ --threads 4 --profiles-dir .
        """
    )

    # -------------------------
    # 4. INTERMEDIATE LAYER
    # -------------------------
    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt build --select intermediate+ --threads 4 --profiles-dir .
        """
    )

    # -------------------------
    # 5. MART LAYER
    # -------------------------
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"""
        cd {DBT_DIR} &&
        dbt build --select marts+ --threads 4 --profiles-dir .
        """
    )

    # -------------------------
    # DEPENDENCY FLOW
    # -------------------------
    dbt_compile >> dbt_staging >> dbt_intermediate >> dbt_marts
