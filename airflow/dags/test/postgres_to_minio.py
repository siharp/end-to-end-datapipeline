from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

MINIO_BUCKET = "my-data"
MINIO_ENDPOINT = "http://minio-server:9000"

with DAG(
    dag_id='dynamic_task_postgres_to_minio',
    start_date=datetime(2026, 5, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    @task
    def get_enabled_tables():
        return ['olist_customers', 'product_category_name_translation', 'olist_sellers']

    @task(map_index_template="{{ task.op_kwargs['table_name'] }}")
    def extract_to_minio(table_name: str, ds=None):
        pg_hook = PostgresHook(postgres_conn_id='local-postgres')

        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )

        year, month, day = ds.split('-')
        s3_key = f"landing/{table_name}/year={year}/month={month}/day={day}/{table_name}.parquet"

        query = f"SELECT * FROM staging.{table_name}"

        # ✅ Gunakan psycopg2 cursor langsung — tidak lewat pandas/SQLAlchemy
        conn = pg_hook.get_conn()       # psycopg2 connection
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        df = pd.DataFrame(rows, columns=columns)

        if not df.empty:
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)

            s3_client.put_object(
                Bucket=MINIO_BUCKET,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            return {"table": table_name, "status": "success", "path": s3_key}

        return {"table": table_name, "status": "empty"}

    target_tables = get_enabled_tables()
    extraction_results = extract_to_minio.expand(table_name=target_tables)
