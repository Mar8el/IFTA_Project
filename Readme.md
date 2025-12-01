# 📦 IFTA Analytics Pipeline — End-to-End Modern Data Engineering Project


This project is a complete, Dockerized,  data pipeline that processes mileage & fuel usage for generating IFTA (International Fuel Tax Agreement) reports.

It demonstrates real-world data engineering skills, including ingestion, orchestration, storage, transformations, and medallion architecture using Airflow, MinIO, PostgreSQL, dbt, and FastAPI.

## 🚀 Architecture Overview

FastAPI → Airflow → MinIO → Postgres → dbt → Gold Tables → Visualization

### Components

**FastAPI** – Generates synthetic truck mileage data (similar to Samsara, GeoTab, Fleetio).

**MinIO** – S3-compatible storage for raw JSON & uploaded CSV fuel files.

**Airflow** – 3 DAGs orchestrating ingestion and transformation:

**_1st Dag_** - Fetches mileage data from API then store JSON in MinIO + appends to Postgres.

**_2nd Dag_** - Reads fuel CSV from MinIO then loads into Postgres.

**_3rd Dag_** - Run Dbt models

**PostgreSQL** – Raw, Silver, and Gold tables.

**dbt** – 5 models implementing a medallion architecture:

**_Bronze_**: raw API mileage + raw fuel CSV

**_Silver_**: cleaned mileage & cleaned fuel

**_Gold_**: aggregated mileage, aggregated fuel, and final joined table


**pgAdmin** – UI for inspecting Postgres.

Docker Compose – Everything runs locally, fully isolated.

git clone https://github.com/Mar8el/IFTA_Project.git
cd IFTA_Project
docker compose up
