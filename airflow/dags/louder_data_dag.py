from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os
import requests
import datetime

def download_file(url: str, path: str):
    """Завантажує один файл за URL у вказану папку."""
    print(f"Downloading → {path}")

    r = requests.get(url)
    if r.status_code == 200:
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"✔ OK: {os.path.basename(path)}\n")
    else:
        print(f"✖ ERROR: {url} (status {r.status_code})\n")


def download_taxi_zone_lookup(**context):
    """Завантажує taxi_zone_lookup.csv."""
    download_folder = context["params"]["download_folder"]
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    path = os.path.join(download_folder, "taxi_zone_lookup.csv")
    download_file(url, path)


def download_yellow_taxi_2024(**context):
    """Завантажує Yellow Taxi за 2024 рік."""
    download_folder = context["params"]["download_folder"]
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    months = [f"{i:02d}" for i in range(1, 13)]

    for month in months:
        file_name = f"yellow_tripdata_2024-{month}.parquet"
        url = f"{base_url}/{file_name}"
        path = os.path.join(download_folder, file_name)
        download_file(url, path)


default_args = {
    "start_date": datetime(2024, 1, 1),
}
with DAG(
    dag_id="download_taxi_data",
    schedule_interval = None,
    default_args= default_args,
    catchup= False,
    params= {"download_folder": "data/raw/taxi"},
) as dag:
    task_zone_lookup = PythonOperator(
        task_id= "download_zone_lookup",
        python_callable= download_taxi_zone_lookup

    )

    task_yellow_taxi = PythonOperator(
        task_id= "download_yellow_taxi_2024",
        python_callable=download_yellow_taxi_2024

    )