from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LOAD_DAG_ID = "nbu_exchange_rates_to_bigquery"
DATAFORM_STAGING_DAG_ID = "dataform_staging_pipeline"
DATAFORM_TRANSFORM_DAG_ID = "dataform_gcp_pipeline"


@dag(
    dag_id="nbu_controller_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 1, 11, 0, 0),
    catchup=False,
    tags=["nbu", "controller", "trigger", "master"],
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Дата выгрузки (YYYY-MM-DD или YYYYMMDD). Оставьте пустым для выгрузки за дату запуска.",
        )
    },
    description="Мастер-контроллер: 1. Загрузка в tmp -> 2. Dataform Staging -> 3. Dataform Витрины",
)
def nbu_controller_pipeline():

    # Шаг 1: Выгрузка данных из API НБУ во временную таблицу (tmp/operational)
    trigger_nbu_etl = TriggerDagRunOperator(
        task_id="trigger_nbu_rates_etl",
        trigger_dag_id=LOAD_DAG_ID,
        conf={
            "target_date": "{{ params.target_date if params.target_date else ds }}"
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    # Шаг 2: Запуск 1-го Dataform (загрузка и merge из tmp в stage)
    trigger_dataform_staging = TriggerDagRunOperator(
        task_id="trigger_dataform_staging",
        trigger_dag_id=DATAFORM_STAGING_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    # Шаг 3: Запуск 2-го Dataform (обработка в core и построение витрин в reporting)
    trigger_dataform_transform = TriggerDagRunOperator(
        task_id="trigger_dataform_transform",
        trigger_dag_id=DATAFORM_TRANSFORM_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    # Последовательное выполнение цепочки
    trigger_nbu_etl >> trigger_dataform_staging >> trigger_dataform_transform


nbu_controller_pipeline()
