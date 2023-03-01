from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

dag_id = "test_dag"

def print_console():
    print("test success")

with DAG(
        dag_id=dag_id,
        # start_date=datetime(2022, 4, 10),
        start_date=datetime.strptime(datetime.now(), '%Y-%m-%d'),
        schedule_interval="0 5 * * *",
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