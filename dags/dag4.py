from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from DAG 4!")

with DAG(
    dag_id="dag4",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag4:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )
