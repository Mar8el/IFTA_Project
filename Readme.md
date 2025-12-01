# 📦 IFTA Analytics Pipeline — End-to-End Modern Data Engineering Project


This project is a complete, Dockerized,  data pipeline that processes mileage & fuel usage for generating IFTA (International Fuel Tax Agreement) reports.

It demonstrates real-world data engineering skills, including ingestion, orchestration, storage, transformations, and medallion architecture using Airflow, MinIO, PostgreSQL, dbt, and FastAPI.

## 🚀 Architecture Overview

FastAPI → Airflow → MinIO → Postgres → dbt → Gold Tables → Visualization

## Components

**FastAPI** – Generates synthetic truck mileage data (similar to Samsara, GeoTab, Fleetio).

**MinIO** – S3-compatible storage for raw JSON & uploaded CSV fuel files.

**Airflow** – 3 DAGs orchestrating ingestion and transformation:

 -  **_1st Dag_** - Fetches mileage data from API then store JSON in MinIO + appends to Postgres.

 -  **_2nd Dag_** - Reads fuel CSV from MinIO then loads into Postgres.

 -  **_3rd Dag_** - Run Dbt models

**PostgreSQL** – Raw, Silver, and Gold tables.

**DBT** – 5 models implementing a medallion architecture:

 -  **_Bronze_**: raw API mileage + raw fuel CSV

 -  **_Silver_**: cleaned mileage & cleaned fuel

 -  **_Gold_**: aggregated mileage, aggregated fuel, and final joined table


**pgAdmin** – UI for inspecting Postgres.



**Docker Compose** – Everything runs locally, fully isolated.

## 🏗 Medallion Architecture

### Bronze Layer

**ifta_raw** — API mileage per vehicle (48 state columns × 500 vehicles).

**fuel_data** — CSV fuel transactions (400 rows manually uploaded to MinIO).


### Silver Layer

**mileage_silver** — Sum of all mileage by state, converted from meters → miles.

**fuel_silver** — Cleaned fuel data (gallons + total cost only).


### Gold Layer

**mileage_gold** — Unpivot mileage to 48 rows (state, mileage).

**fuel_gold** — Sum of gallons & cost per state.

**join** — Joined table of mileage + gallons per state (48 rows). 

## 📊 Final Deliverable

A clean, analytics-ready dataset. 

Report is ready.

This can be visualized using Apache Superset, Power BI, or Tableau.

|State|total_mileage|total_fuel|
|AL|	6132|	541|
|AR|	6330|	374|
|AZ|	6766|	786|
|CA|	6523|	1323|
|CO|	7796|	764|
|CT|	7154|	431|
|DE|	6599|	810|

## Test It Out

1 - Clone the repository: 
``` 
git clone https://github.com/Mar8el/IFTA_Project.git
```
2 - Navigate to the project directory 
```
cd IFTA_Project
```
3 - Local deployment 
```
docker compose up
```
