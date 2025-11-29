from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="run_dbt_everyday_1159pm",
    schedule="59 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_all",
        bash_command="""
            docker exec dbt_ifta \
                dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt
        """.strip(),
    )