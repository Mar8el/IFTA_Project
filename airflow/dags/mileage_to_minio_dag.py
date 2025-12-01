from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import boto3
import json
import psycopg2
from airflow.models.taskinstance import TaskInstance


STATE_COLUMNS = [
    "AL","AR","AZ","CA","CO","CT","DE","FL","GA","ID","IL","INDIANA","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OREGON","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

def call_api_and_save_to_minio():

    # 1. MinIO client setup
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin_ifta",
        aws_secret_access_key="minioadmin_ifta",
        region_name="us-east-1"
    )

    # 2. Call FastAPI (fails on bad status)
    response = requests.get("http://host.docker.internal:8005/abcd?vehicles=500")
    response.raise_for_status()
    data = response.json()

    # 3. Save JSON to MinIO
    file_key = f"truck_miles/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    s3.put_object(
        Bucket="ifta-raw",
        Key=file_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    

    return file_key

def load_data_from_minio_to_postgres(ti: TaskInstance):

    minio_file_key = ti.xcom_pull(task_ids='call_api_and_save_to_minio', key='return_value')


    if not minio_file_key:
        raise ValueError("MinIO file key not found in XCom.")


    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin_ifta",
        aws_secret_access_key="minioadmin_ifta",
        region_name="us-east-1"
    )
    obj = s3.get_object(Bucket="ifta-raw", Key=minio_file_key)
    data = json.loads(obj["Body"].read().decode("utf-8"))

    # 3. Connect to Postgres
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="db",
        user="db_user",
        password="db_password"
    )
    cur = conn.cursor()

    # 4. Create table if it does not exist
    state_schema = ", ".join([f"{state} FLOAT" for state in STATE_COLUMNS])
    cur.execute(f"CREATE TABLE IF NOT EXISTS ifta (vehicle_id TEXT, total_meters FLOAT, {state_schema});")

    # 5. Prepare and execute inserts
    columns = ["vehicle_id", "total_meters"] + STATE_COLUMNS
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO ifta ({', '.join(columns)}) VALUES ({placeholders});"

    for rec in data:
        state_values = {s: None for s in STATE_COLUMNS}
        for s_entry in rec.get("per_state", []):
            state = s_entry.get("state")
            meters = s_entry.get("meters")
            if state in state_values:
                state_values[state] = meters if meters is not None else 0.0

        values = [rec["vehicle_id"], rec.get("total_meters", 0.0)] + [state_values[s] for s in STATE_COLUMNS]
        cur.execute(insert_sql, values)

    conn.commit()
    cur.close()
    conn.close()



with DAG(
    dag_id="from_API_to_minio_n_postgres",
    start_date=datetime(2025, 1, 1),
    schedule="57 23 * * *",  # every 5 minutes
    catchup=False,
    tags=["ifta", "minio", "postgres"]
) as dag:

    save_to_minio_task = PythonOperator(
        task_id="call_api_and_save_to_minio",
        python_callable=call_api_and_save_to_minio,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_from_minio_to_postgres,
    )


    save_to_minio_task >> load_to_postgres_task
