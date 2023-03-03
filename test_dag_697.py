from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

dag_id = "test_dag_697"

def print_console():
    print("697 test start..")
    for i in range(10000):
        pass
    print("697 test success")

with DAG(
        dag_id=dag_id,
        start_date=datetime(2023, 2, 28),
        #start_date=datetime.strptime(datetime.now(), '%Y-%m-%d'),
        schedule_interval="0 * * * *",
        max_active_runs=1,
        tags=["test"]
) as dag:

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")

    load_schema_task = PythonOperator(
        task_id='print_test',
        python_callable=print_console,
    )

    task_start >> load_schema_task >> task_end