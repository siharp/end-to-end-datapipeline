import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import clickhouse_connect
from notifications.telegram import notify_failure, notify_success

log = logging.getLogger(__name__)

SQL_DIR = '/opt/airflow/plugins/sql'


if not os.path.exists(SQL_DIR):
    raise FileNotFoundError(f"SQL_DIR tidak ditemukan: {SQL_DIR}")

sql_files = sorted([f for f in os.listdir(SQL_DIR) if f.endswith('.sql')])

if not sql_files:
    raise ValueError(f"Tidak ada file .sql ditemukan di {SQL_DIR}")


ENV_CONFIG = {
    'dev': {
        'clickhouse_variable': 'clickhouse_config_dev',
        'schedule': None,
        'max_active_tasks': 2,
    },
    'staging': {
        'clickhouse_variable': 'clickhouse_config_staging',
        'schedule': None,
        'max_active_tasks': 2,
    },
    'prod': {
        'clickhouse_variable': 'clickhouse_config_prod',
        'schedule': None,
        'max_active_tasks': 4,
        'retries': 3,
    }
}

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': notify_failure,
}


def create_pipeline_dag(env: str, config: dict):
    @dag(
        dag_id=f'create_table_clickhouse_{env}',
        start_date=datetime(2026, 5, 1),
        schedule_interval=config['schedule'],
        catchup=False,
        max_active_tasks=config['max_active_tasks'],
        tags=['clickhouse', 'ddl', 'setup', env],
        on_success_callback=notify_success,
        default_args=default_args)
    def create_table_clickhouse():
        start = EmptyOperator(task_id='start')
        end = EmptyOperator(task_id='end')

        @task
        def run_ch_sql(file_path: str):
            ch_conf = Variable.get(
                config['clickhouse_variable'], deserialize_json=True)
            client = clickhouse_connect.get_client(
                host=ch_conf.get('host'),
                port=ch_conf.get('port'),
                username=ch_conf.get('username'),
                password=ch_conf.get('password'),
                database=ch_conf.get('database')
            )

            try:
                try:
                    with open(file_path, 'r') as f:
                        query = f.read().strip()
                except OSError as e:
                    raise RuntimeError(
                        f"Gagal membaca file: {file_path}") from e

                if not query:
                    raise ValueError(f"File SQL kosong: {file_path}")

                table_name = os.path.basename(file_path).replace('.sql', '')
                log.info(f"Executing DDL for table: {table_name}")
                log.info(f"Query: {query}")

                try:
                    client.command(query)
                    log.info(f"Berhasil membuat tabel: {table_name}")
                except Exception as e:
                    raise RuntimeError(
                        f"Gagal eksekusi DDL untuk {table_name}: {e}") from e

            finally:
                client.close()

        for sql_file in sql_files:
            full_path = os.path.join(SQL_DIR, sql_file)
            task_id = f"create_table_{sql_file.replace('.sql', '')}"

            sql_task = run_ch_sql.override(
                task_id=task_id)(file_path=full_path)
            start >> sql_task >> end

    return create_table_clickhouse()


for env, config in ENV_CONFIG.items():
    create_pipeline_dag(env=env, config=config)
