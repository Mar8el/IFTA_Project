"""
DAG: minio_to_postgres_fuel
Hard-coded MinIO + PostgreSQL (psycopg2)
No Airflow connections needed
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import io
import psycopg2
import logging

# ----------------------------------------------------------------------
# CONFIG – HARD-CODED
# ----------------------------------------------------------------------
# MinIO
MINIO_ENDPOINT          = "http://minio:9000"
MINIO_ACCESS_KEY        = "minioadmin_ifta"
MINIO_SECRET_KEY        = "minioadmin_ifta"
MINIO_BUCKET            = "fuel"
MINIO_KEY               = "fuel_data.csv"

# PostgreSQL
PG_HOST                 = "postgres"
PG_PORT                 = 5432
PG_DB                   = "db"
PG_USER                 = "db_user"
PG_PASSWORD             = "db_password"
TARGET_TABLE            = "fuel_data"


# ----------------------------------------------------------------------
# TASK 1 – Create table
# ----------------------------------------------------------------------
def create_fuel_table(**kwargs):
    logging.info("Creating table with psycopg2.connect(...)")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        transaction_id   VARCHAR(200),
        vehicle_id       VARCHAR(50),
        date             DATE,
        city             VARCHAR(100),
        state            VARCHAR(50),
        diesel_gallons   DECIMAL(12,3),
        fuel_type        VARCHAR(50),
        price_per_gallon DECIMAL(10,4),
        DEF_gallons      DECIMAL(12,3)
    );
    """
    try:
        cur.execute(create_sql)
        conn.commit()
        logging.info(f"Table `{TARGET_TABLE}` created/ensured.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Create table failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ----------------------------------------------------------------------
# TASK 2 – Copy CSV from MinIO → PostgreSQL
# ----------------------------------------------------------------------
def copy_minio_to_postgres(**kwargs):
    logging.info("Downloading CSV from MinIO...")

    # ----- boto3 S3 client (hard-coded) -----
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # ----- Download CSV -----
    try:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=MINIO_KEY)
        csv_text = obj["Body"].read().decode("utf-8")
        logging.info(f"Downloaded {len(csv_text.splitlines())} lines from {MINIO_KEY}")
    except Exception as e:
        logging.error(f"Failed to download from MinIO: {e}")
        raise

    # ----- PostgreSQL COPY -----
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = conn.cursor()

    try:
        cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
        logging.info("Table truncated")

        with io.StringIO(csv_text) as f:
            cur.copy_expert(
                f"COPY {TARGET_TABLE} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
                f
            )
        conn.commit()
        logging.info(f"CSV successfully loaded into `{TARGET_TABLE}`")
    except Exception as e:
        conn.rollback()
        logging.error(f"COPY failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------
with DAG(
    dag_id="minio_to_postgres_fuel",
    start_date=datetime(2024, 1, 1),
    schedule="58 23 * * *",           # Airflow 2.8+
    catchup=False,
    tags=["minio", "postgres", "fuel"],
    default_args={"retries": 2},
    description="Hard-coded MinIO → PostgreSQL fuel data load",
) as dag:

    t_create = PythonOperator(
        task_id="create_table",
        python_callable=create_fuel_table,
    )

    t_load = PythonOperator(
        task_id="load_csv",
        python_callable=copy_minio_to_postgres,
    )

    # Order: create → load
    t_create >> t_load