from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


@dag(
    dag_id='test_minio_connection',
    start_date=datetime(2024, 1, 1),
    schedule='@once',
    catchup=False
)
def test_minio():
    @task
    def check_minio():
        # Ganti dengan Conn ID Anda
        hook = S3Hook(aws_conn_id='minio-connection')

        # Mencoba list semua bucket yang ada
        buckets = hook.get_conn().list_buckets()

        print("--- KONEKSI BERHASIL ---")
        for bucket in buckets['Buckets']:
            print(f"Ditemukan Bucket: {bucket['Name']}")

    check_minio()


test_minio()
