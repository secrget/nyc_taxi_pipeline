from datetime import datetime
from airflow import DAG
from  airflow.providers.standard.operators.python import PythonOperator

def hello_world():
    print("Привіт, Airflow! DAG працює коректно.")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
):
    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )

    task_hello
