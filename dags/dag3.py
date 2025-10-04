from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def say_hello():
    time.sleep(60)
    print("Hello from DAG 3!")

with DAG(
    dag_id="dag3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag3:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )
