from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests
import pandas as pd
import boto3
import json
import os

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

CITY = "Seoul"

def fetch_weather_data():

    print("API_KEY =", API_KEY)

    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

    print(url)

    response = requests.get(url)

    print(response.text)

    data = response.json()

    with open("/tmp/weather.json", "w") as f:
        json.dump(data, f)

def transform_to_csv():

    with open("/tmp/weather.json", "r") as f:
        data = json.load(f)

    print(data)

    if "name" not in data:
        raise Exception(f"API 응답 오류: {data}")

    transformed_data = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"],
        "collected_at": datetime.now().isoformat()
    }

    df = pd.DataFrame([transformed_data])

    df.to_csv("/tmp/weather.csv", index=False)

def upload_to_s3():

    s3 = boto3.client(
        "s3",
        region_name="ap-northeast-2"
    )

    today = datetime.now().strftime("%Y-%m-%d")

    s3_key = f"raw/weather/{today}/weather.csv"

    s3.upload_file(
        "/tmp/weather.csv",
        BUCKET_NAME,
        s3_key
    )

with DAG(
    dag_id="weather_api_to_s3",

    start_date=datetime(2026, 1, 1),

    schedule_interval="@daily",

    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data
    )

    t2 = PythonOperator(
        task_id="transform_to_csv",
        python_callable=transform_to_csv
    )

    t3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    t1 >> t2 >> t3