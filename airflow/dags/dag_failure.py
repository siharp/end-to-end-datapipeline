from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from notifications.email import dag_failure_callback, dag_success_callback


def always_fail():
    raise Exception("Task sengaja dibuat gagal")


with DAG(
    dag_id="email_notification_demo",

    start_date=datetime(2025, 1, 1),

    schedule=None,

    catchup=False,

    on_failure_callback=dag_failure_callback,

    on_success_callback=dag_success_callback,

    tags=["email"],
) as dag:

    task1 = PythonOperator(
        task_id="task_test",
        python_callable=always_fail
    )
