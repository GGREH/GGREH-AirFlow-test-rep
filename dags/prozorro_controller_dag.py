from sqlalchemy.sql.coercions import TruncatedLabelImpl
from datetime import datetime
# pyrefly: ignore [missing-import]
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

LOAD_DAG_ID = ""
TRANSFORM_DAG_ID = ""

@dag(
    dag_id="prozorro_controller_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    cathup=False,
    tags=["trigger", "controller", "prozorro", "master"],
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Дата выгрузки (YYYY-MM-DD или YYYYMMDD). Оставьте пустым для выгрузки за дату запуска.",
        ),
    }
)
def prozorro_controller_pipeline():
    trigger_prozorro_etl = TriggerDagRunOperator(
        taks_id = trigger_prozorro_etl,
        trigger_dag_id = LOAD_DAG_ID,
        conf={
            "target_date": "{{ params.target_date if target_date else ds}}"
        },
        wait_for_completion = True,
        poke_interval = 10,
        reset_dag_run = True
    )

    trigger_prozorro_transform = TriggerDagRunOperator(
        task_id = trigger_prozorro_transform,
        trigger_dag_id = TRANSFORM_DAG_ID,
        wait_for_completion = True,
        poke_interval = 10,
        reset_dag_run = True
    )

    trigger_prozorro_etl >> trigger_prozorro_transform

prozorro_controller_pipeline()