from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_DAG_ID = "nbu_exchange_rates_to_bigquery"


@dag(
    dag_id="nbu_controller_dag",
    schedule="@daily",  # Расписание запуска управляющего DAG (или None для ручного запуска)
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["nbu", "controller", "trigger", "master"],
    description="Управляющий DAG: запрашивает дату и триггерит исполняющий DAG nbu_exchange_rates_to_bigquery",
    params={
        "target_date": Param(
            default=datetime.now().strftime("%Y-%m-%d"),
            type="string",
            description="Дата для выгрузки курсов валют в формате YYYY-MM-DD или YYYYMMDD (например, 2026-09-01).",
        )
    },
)
def nbu_controller_pipeline():

    trigger_nbu_etl = TriggerDagRunOperator(
        task_id="trigger_nbu_rates_etl",
        trigger_dag_id=TARGET_DAG_ID,
        conf={
            # Передаем значение параметра target_date в исполняющий DAG
            "target_date": "{{ params.target_date }}"
        },
        wait_for_completion=True,  # Ожидать завершения работы исполняющего DAG
        poke_interval=10,          # Интервал проверки статуса (в секундах)
        reset_dag_run=True,        # Разрешить повторный запуск
    )

    trigger_nbu_etl


nbu_controller_pipeline()
