import io
import logging
import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from notifications.telegram import notify_failure, notify_success

log = logging.getLogger(__name__)

MINIO_CONN_ID = "minio-connection"
POSTGRES_CONN_ID = "postgres-connection"
MINIO_BUCKET = "olist-data"
CHUNK_SIZE = 10000

tables_to_process = [
    'customers', 'product_category_name_translation', 'sellers',
    'geolocation', 'order_items', 'order_reviews',
    'order_payments', 'orders', 'products'
]


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
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure,
}


def create_pipeline_dag(env: str, config: dict):
    @dag(
        dag_id=f'el_postgres_minio_clickhouse_{env}',
        start_date=datetime(2026, 5, 15),
        schedule_interval=config['schedule'],
        catchup=False,
        tags=['postgres', 'minio', 'clickhouse', env],
        max_active_tasks=config['max_active_tasks'],
        on_success_callback=notify_success,
        default_args=default_args
    )
    def el_postgres_minio_clickhouse():
        start_pipeline = EmptyOperator(task_id='start_pipeline')
        end_pipeline = EmptyOperator(task_id='end_pipeline')

        @task_group(group_id='extract_all_tables_to_minio')
        def extract_group():

            @task
            def extract_task(t_name: str):
                context = get_current_context()
                ds = context['ds']
                year, month, day = ds.split('-')
                log.info(f"Mulai extract tabel: {t_name}")
                pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
                conn = pg_hook.get_conn()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f'SELECT * FROM staging."{t_name}"')
                        columns = [desc[0] for desc in cursor.description]
                        chunk_idx = 0
                        while True:
                            rows = cursor.fetchmany(CHUNK_SIZE)
                            if not rows:
                                break
                            chunk_idx += 1
                            df_chunk = pd.DataFrame(rows, columns=columns)
                            buffer = io.BytesIO()
                            df_chunk.to_parquet(
                                buffer, index=False, engine='pyarrow')
                            buffer.seek(0)
                            chunk_key = f"landing/{env}/{t_name}/{year}/{month}/{day}/{t_name}_part_{chunk_idx:03d}.parquet"
                            s3_hook.load_file_obj(
                                file_obj=buffer, key=chunk_key,
                                bucket_name=MINIO_BUCKET, replace=True
                            )
                            log.info(f"Upload chunk {chunk_idx} → {chunk_key}")
                finally:
                    conn.close()
                log.info(f"Selesai extract {t_name}, total chunk: {chunk_idx}")

            for table in tables_to_process:
                extract_task.override(task_id=f"extract_{table}")(table)

        @task_group(group_id='load_all_tables_to_clickhouse')
        def load_group():

            @task
            def load_task(t_name: str):
                context = get_current_context()
                ds = context['ds']
                year, month, day = ds.split('-')

                log.info(f"Mulai load tabel: {t_name}")

                ch_conf = Variable.get(
                    config['clickhouse_variable'], deserialize_json=True)
                s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
                creds = s3_hook.get_credentials()
                db_name = ch_conf.get('database')

                client = None
                try:
                    client = clickhouse_connect.get_client(
                        host=ch_conf.get('host'),
                        port=ch_conf.get('port', 8123),
                        username=ch_conf.get('username'),
                        password=ch_conf.get('password'),
                        database=db_name
                    )
                    try:
                        s3_path = (
                            f"http://minio-server:9000/{MINIO_BUCKET}/landing/{env}/"
                            f"{t_name}/{year}/{month}/{day}/{t_name}_part_*.parquet"
                        )
                        query = (
                            f"INSERT INTO {db_name}.{t_name} "
                            f"SELECT * FROM s3('{s3_path}', "
                            f"'{creds.access_key}', '{creds.secret_key}', 'Parquet')"
                        )
                        log.info(f"S3 path: {s3_path}")
                        log.info(f"Query: {query}")

                        client.command(query)
                        log.info(f"Berhasil load tabel: {t_name}")
                    except Exception as e:
                        raise RuntimeError(
                            f"Gagal load tabel {t_name}: {e}") from e

                finally:
                    if client:
                        client.close()

            for table in tables_to_process:
                load_task.override(task_id=f"load_{table}")(table)

        extract_tasks = extract_group()
        load_tasks = load_group()

        start_pipeline >> extract_tasks >> load_tasks >> end_pipeline

    return el_postgres_minio_clickhouse()


for env, config in ENV_CONFIG.items():
    create_pipeline_dag(env=env, config=config)
