# pyrefly: ignore [missing-import]
from datetime import datetime
from airflow.decorators import @dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LOAD_DAG_ID = "prozorro_etl_pipeline"
DATAFORM_STAGING_DAG_ID = "prozorro_dataform_staging_pipeline"
DATAFORM_TRANSFORM_DAG_ID = "prozorro_dataform_transform_pipeline"

@dag(
    dag_id ="prozorro_controller_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 1, 11, 0, 0),
    catchup=False,
    tags=["prozorro", "controller", "trigger", "master"],
    params={
        "target_date": Param(
            default=None, 
            type=["null", "string"], 
            description="Дата выгрузки (YYYY-MM-DD или YYYYMMDD). Оставьте пустым для выгрузки за дату запуска."
            )
    },
    description = "Мастер-контроллер: 1. Загрузка в tmp -> 2. Dataform Staging -> 3. Dataform Витрины"
)
def prozorro_pipeline():
    trigger_prozorro_etl = TriggerDagRunOperator(
        trigger_dag_id=LOAD_DAG_ID,
        conf={
            "target_date": "{{ params.target_date if params.target_date else ds }}"
        },
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )
    trigger_dataform_staging = TriggerDagRunOperator(
        trigger_dag_id = DATAFORM_STAGING_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )
    trigger_dataform_transform = TriggerDagRunOperator(
        trigger_dag_id=DATAFORM_TRANSFORM_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )

    trigger_prozorro_etl >> trigger_dataform_staging >> trigger_dataform_transform

prozorro_pipeline()