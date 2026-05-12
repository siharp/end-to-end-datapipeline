# End-to-End Modern Data Pipeline: Olist E-Commerce Analytics

This project implements a comprehensive **Modern Data Stack (MDS)** to process, transform, and visualize the [Olist E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The architecture is designed to handle analytical workloads efficiently by leveraging columnar storage and automated orchestration.

---

## 🏗️ Data Architecture

The pipeline follows a robust **ELT (Extract, Load, Transform)** workflow:

```
PostgreSQL (OLTP)
      │
      │  Extract & Load 
      ▼
  MinIO (S3 / Parquet)
      │
      │  Load into OLAP
      ▼
  ClickHouse (Data Warehouse)
      │
      ├──► dbt (Transform: Staging → Core → Marts)
      │
      └──► Apache Superset (Dashboards & Visualization)

        ⬆ Orchestrated & Scheduled by Apache Airflow
```

| Step | Component | Description |
|------|-----------|-------------|
| 1 | **PostgreSQL** | Source of transactional data (Schema: `staging`) |
| 2 | **MinIO** | S3-compatible object storage — staging area |
| 3 | **ClickHouse** | High-performance OLAP engine (Data Warehouse) |
| 4 | **dbt** | Data modeling — Staging, Core, Marts layers (T) |
| 5 | **Apache Superset** | Interactive BI dashboards connected to ClickHouse |
| 6 | **Apache Airflow** | Orchestrates, schedules, and monitors the full pipeline |

---

## 🚀 Tech Stack

| Category | Tool |
|----------|------|
| **OLTP Database** | PostgreSQL 15+ |
| **OLAP / Data Warehouse** | ClickHouse |
| **Object Storage** | MinIO (S3-Compatible) |
| **Transformation** | dbt-core (`dbt-clickhouse` adapter) |
| **Orchestration** | Apache Airflow |
| **Visualization** | Apache Superset |
| **Infrastructure** | Docker & Docker Compose |

---

## 🛠️ Setup & Installation

### Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- Python 3.10+
- `make` *(optional, for automation shortcuts)*

### 1. Clone the Repository

```bash
git clone https://github.com/siharp/end-to-end-datapipeline.git
cd end-to-end-datapipeline
```

### 2. Spin Up All Services

Launch the entire stack with a single command:

```bash
make up
```

> This will start PostgreSQL, MinIO, ClickHouse, Airbyte, Airflow, and Superset via Docker Compose.

### 3. Initial Data Ingestion

Upload the Olist CSV samples into the PostgreSQL source database:

```bash
python3 ./data-example/upload-data-to-postgres.py
```

---

## 📂 Directory Structure

```
olist-data-engineering/
├── dbt_project/          # dbt models, macros, and tests
├── docker/               # Dockerfiles and Compose configurations
├── scripts/              # Python scripts for data automation
├── data_samples/         # Olist sample datasets (.csv)
├── Makefile              # Automation commands (up, down, ingest)
└── README.md
```

---

## 📊 Pipeline Details

### Ingestion

Python script is configure to load data from PostgreSQL to MinIO.

- **Format:** Apache Parquet
- **Why Parquet?** Columnar format ensures optimal storage efficiency and significantly faster read speeds during the ClickHouse loading phase.

---

### Transformation — dbt

The transformation logic is organized into **three layers**:

```
Raw (MinIO/ClickHouse)
      │
      ▼
  Staging        → Initial cleanup, renaming, type casting
      │
      ▼
  Intermediate   → Complex joins and business logic
      │
      ▼
  Marts          → Analytics-ready tables (fct_orders, dim_products, ...)
```

---

### Analytics — Apache Superset

Final dashboards provide insights into:

---


> **Author:** Sihar Pangaribuan