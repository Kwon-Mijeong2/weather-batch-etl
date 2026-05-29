from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def hello():
    print("Weather ETL Start!")

with DAG(
    dag_id="test_weather_pipeline",
    start_date=datetime(2026, 5, 28),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    task1