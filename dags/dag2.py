# dag2.py
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG("dag2", start_date=datetime(2025, 1, 1), schedule="@daily", catchup=False) as dag2:
    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="dag3",
    )
