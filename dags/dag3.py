# dag3.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from DAG 3!")

with DAG(
    "dag3",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag3:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )
