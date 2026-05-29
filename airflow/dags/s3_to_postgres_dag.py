from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import boto3
import pandas as pd
import psycopg2
import os

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def download_from_s3():

    s3 = boto3.client("s3")

    today = datetime.now().strftime("%Y-%m-%d")

    s3_key = f"raw/weather/{today}/weather.csv"

    s3.download_file(
        BUCKET_NAME,
        s3_key,
        "/tmp/weather.csv"
    )

def load_to_postgres():

    df = pd.read_csv("/tmp/weather.csv")

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    cur = conn.cursor()

    # 테이블 없으면 자동 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            humidity INT,
            weather VARCHAR(100),
            wind_speed FLOAT,
            collected_at TIMESTAMP
        )
    """)

    conn.commit()

    for _, row in df.iterrows():

        cur.execute(
            """
            INSERT INTO weather_raw (
                city,
                temperature,
                humidity,
                weather,
                wind_speed,
                collected_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["city"],
                row["temperature"],
                row["humidity"],
                row["weather"],
                row["wind_speed"],
                row["collected_at"]
            )
        )

    conn.commit()

    cur.close()
    conn.close()

with DAG(
    dag_id="s3_to_postgres_weather",

    start_date=datetime(2026, 1, 1),

    schedule_interval="@daily",

    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3
    )

    t2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    t1 >> t2