from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Функция, которая будет выполнять основную работу
def print_current_datetime():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Текущая дата и время: {current_time}")

# Настройки по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Определение DAG
with DAG(
    dag_id='print_datetime_dag',
    default_args=default_args,
    description='Простой DAG для вывода текущей даты и времени',
    schedule='@daily',  # Запуск каждый день (можно изменить, например, на None или Cron)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Определение задачи с использованием PythonOperator
    print_time_task = PythonOperator(
        task_id='print_time_task',
        python_callable=print_current_datetime,
    )