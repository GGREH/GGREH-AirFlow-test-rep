from requests.sessions import default_headers
from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_DAG_ID = "nbu_exchange_rates_to_bigquery"
TRANSFORM_DAG_ID = "dataform_gcp_pipeline"


@dag(
    dag_id="nbu_controller_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["nbu", "controller", "trigger", "master"],
    params={
        "target_date": Param(
            default=None,
            type=["null", "string"],
            description="Дата выгрузки (YYYY-MM-DD или YYYYMMDD). Оставьте пустым для выгрузки за дату запуска.",
        )
    }
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

    trigger_dataform = TriggerDagRunOperator(
        task_id="trigger_dataform_transform",
        trigger_dag_id=TRANSFORM_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    trigger_nbu_etl >> trigger_dataform


nbu_controller_pipeline()
