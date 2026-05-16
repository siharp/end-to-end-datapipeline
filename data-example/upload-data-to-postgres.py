import os
import pandas as pd
from sqlalchemy import create_engine, text

# Koneksi ke database (Pastikan port 5438 sudah benar di mapping ke Docker)
engine = create_engine('postgresql://sihar:sihar123@localhost:5438/olist')

# --- TAMBAHAN: Pastikan schema 'staging' ada ---
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    conn.commit()
# ----------------------------------------------

for item in os.listdir():
    if item.endswith(".csv"):
        try:
            print(f"Uploading data {item} to database...")

            # Membaca CSV
            df = pd.read_csv(item)

            # Cleaning nama tabel (contoh: 'orders_dataset.csv' -> 'orders')
            table_name = item.split('.')[1:].replace('_dataset', '')

            # Upload menggunakan Pandas
            df.to_sql(
                con=engine,
                name=table_name,
                if_exists="replace",
                index=False,
                schema='staging',
                # Menangani file besar (seperti olist_order_items)
                chunksize=30000,
                method='multi'   # Mempercepat proses insert
            )
            print(f"Success upload data '{table_name}' to schema 'staging'")

        except Exception as e:
            print(f"Failed to upload {item}. Error: {e}")

print("\nSemua proses selesai!")
