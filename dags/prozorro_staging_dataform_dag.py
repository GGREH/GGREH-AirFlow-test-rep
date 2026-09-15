from datetime import datetime
from airflow.decorators import @dag
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = "project-4deacada-3830-4d03-80c"
REGION = "us-central1"
STAGE_REPOSITORY_ID = "prozorro_stage"
STAGE_WORKSPACE_ID = "prozorro_tenders_stage"

STAGE_WORKSPACE_PATH = (
    f"projects/{PROJECT_ID}/locations/{REGION}/"
    f"repositories/{STAGE_REPOSITORY_ID}/workspaces/{STAGE_WORKSPACE_ID}"
)

@dag(
    dag_id = "prozorro_daraform_staging_pipeline",
    start_date=datetime(2026, 9, 1, 11, 0, 0),
    schedule=None,
    catchup=False,
    tags = ["prozorro", "dataform", "staging", "gcp", "worker", "child"],
    description="Исполняющий DAG 1: инкрементальная загрузка данных из prozorro (tmp) в prozorro_staging",
)
def prozorro_dataform_staging_pipeline():
    complise_staging = DataformCreateCompilationResultOperator(
        task_id = "compile_staging_dataform",
        project_id = PROJECT_ID,
        region = REGION,
        repository_id = STAGE_REPOSITORY_ID,
        compilation_result = {
            "workspace": STAGE_WORKSPACE_PATH
        },
    )
    invoke_staging = DataformCreateWorkflowInvocationOperator(
        task_id = "invoke_staging_dataform",
        project_id = PROJECT_ID,
        region = REGION,
        repository_id = STAGE_REPOSITORY_ID,
        workflow_invocation = {
            "compilation_result": "{{ task_instance.xcom_pull('compile_staging_dataform'['name']) }}",
        },
    )

    complise_staging >> invoke_staging

prozorro_dataform_staging_pipeline()