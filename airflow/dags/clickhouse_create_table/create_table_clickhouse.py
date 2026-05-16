import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
import clickhouse_connect

# --- KONFIGURASI PATH DINAMIS ---
SQL_DIR = '/opt/airflow/plugins/sql'

with DAG(
    dag_id='create_table_clickhouse',
    start_date=datetime(2024, 5, 1),
    schedule_interval='@once',
    catchup=False,
    max_active_tasks=2,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def run_ch_sql(file_path: str):
        ch_conf = Variable.get("clickhouse_config", deserialize_json=True)
        client = clickhouse_connect.get_client(
            host=ch_conf.get('host'),
            port=ch_conf.get('port', 8123),
            username=ch_conf.get('username'),
            password=ch_conf.get('password'),
            database=ch_conf.get('database')
        )

        with open(file_path, 'r') as f:
            query = f.read()

        table_name = os.path.basename(file_path).replace('.sql', '')
        print(f"Executing DDL for table: {table_name}")
        client.command(query)

    # Membaca file SQL dari folder di luar dags
    if os.path.exists(SQL_DIR):
        sql_files = [f for f in os.listdir(SQL_DIR) if f.endswith('.sql')]

        for sql_file in sorted(sql_files):
            full_path = os.path.join(SQL_DIR, sql_file)
            task_id = f"create_table_{sql_file.replace('.sql', '')}"

            # Buat task
            sql_task = run_ch_sql.override(
                task_id=task_id)(file_path=full_path)

            start >> sql_task >> end
    else:
        # Jika folder tidak terbaca (biasanya karena belum di-mount di Docker)
        print(f"EROR: Folder SQL tidak ditemukan di {SQL_DIR}")
