from datetime import datetime
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# Конфигурация GCP и первого репозитория Dataform (Tmp -> Staging)
PROJECT_ID = "project-4deacada-3830-4d03-80c"
REGION = "us-central1"
STAGE_REPOSITORY_ID = "nbu_stage"  # Имя репозитория GCP Dataform для Staging
STAGE_WORKSPACE_ID = "nbu_kurs_stage"  # Имя Workspace в GCP (или 'default')

STAGE_WORKSPACE_PATH = (
    f"projects/{PROJECT_ID}/locations/{REGION}/"
    f"repositories/{STAGE_REPOSITORY_ID}/workspaces/{STAGE_WORKSPACE_ID}"
)


@dag(
    dag_id="dataform_staging_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["nbu", "dataform", "staging", "gcp", "worker", "child"],
    description="Исполняющий DAG 1: инкрементальная загрузка данных из nbu_operational (tmp) в nbu_staging",
)
def dataform_staging_pipeline():

    # 1. Компиляция Staging Dataform репозитория
    compile_staging = DataformCreateCompilationResultOperator(
        task_id="compile_staging_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=STAGE_REPOSITORY_ID,
        compilation_result={
            "workspace": STAGE_WORKSPACE_PATH,
        },
    )

    # 2. Вызов выполнения Staging Dataform
    invoke_staging = DataformCreateWorkflowInvocationOperator(
        task_id="invoke_staging_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=STAGE_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_staging_dataform')['name'] }}",
        },
    )

    compile_staging >> invoke_staging


dataform_staging_pipeline()
