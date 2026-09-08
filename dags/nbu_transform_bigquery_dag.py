from datetime import datetime
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = "project-4deacada-3830-4d03-80c"
REGION = "us-central1"
REPOSITORY_ID = "post_stage"
WORKSPACE_ID = "nbu_kurs"

WORKSPACE_PATH = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"


@dag(
    dag_id="dataform_gcp_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["nbu", "dataform", "transform", "gcp", "worker", "child"],
    description="Исполняющий DAG: компилирует и запускает трансформацию Dataform напрямую из GCP Workspace nbu_kurs",
)
def dataform_pipeline():

    # 1. Компиляция Dataform напрямую из Workspace в GCP
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "workspace": WORKSPACE_PATH,
        },
    )

    # 2. Запуск выполнения моделей Dataform по тегу
    invoke_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="invoke_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {
                "included_tags": ["daily_transform"],
            },
        },
    )

    compile_dataform >> invoke_dataform


dataform_pipeline()
