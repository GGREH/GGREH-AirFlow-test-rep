from datetime import datetime
from print_datetime_dag import default_args
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_Hello_World():
    print("Hello World ma boi<3")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    dag_id = 'Hello World DAG',
    default_args = default_args,
    retries = 3,
    start_date = datetime(2026, 8, 28),
    schedule='@hourly',
    catchup = False,
    tags = ['airflow', 'example', 'my_first_dag']
) as dag:
    print_hello = PythonOperator(
        task_id = "Print_Hello_Wolrd_task",
        python_callable = print_Hello_World
    )