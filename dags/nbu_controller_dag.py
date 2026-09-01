from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_DAG_ID = "nbu_exchange_rates_to_bigquery"


@dag(
    dag_id="nbu_controller_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["nbu", "controller", "trigger", "master"],
    description="Управляющий DAG: запрашивает дату и триггерит исполняющий DAG nbu_exchange_rates_to_bigquery",
    params={
        "target_date": Param(
            default="",
            type=["null", "string"],
            description="Дата выгрузки (YYYY-MM-DD или YYYYMMDD). Оставьте пустым для выгрузки за дату запуска.",
        )
    },
)
def nbu_controller_pipeline():

    trigger_nbu_etl = TriggerDagRunOperator(
        task_id="trigger_nbu_rates_etl",
        trigger_dag_id=TARGET_DAG_ID,
        conf={
            "target_date": "{{ params.target_date if params.target_date else ds }}"
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_nbu_etl


nbu_controller_pipeline()
