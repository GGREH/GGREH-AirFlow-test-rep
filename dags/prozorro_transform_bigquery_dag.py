from datetime import datetime
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataform import  (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = "project-4deacada-3830-4d03-80c"
REGION = "us-central1"
REPOSITORY_ID = "prozorro_transform"
WORKSPACE_ID = "prozorro_tenders"

WORKSPACE_PATH = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"

@dag(
    dag_id="prozorro_dataform_transform_pipeline",
    start_date=datetime(2026, 9, 1, 11, 0, 0),
    schedule=None,
    catchup=False,
    tags=["prozorro", "dataform", "transform", "gcp", "worker", "child"],
    description="Исполняющий DAG 2: компилирует и запускает трансформацию Dataform ProZorro (Core & Reporting витрины)",
)
def prozorro_dataform_transform_pipeline():
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "workspace": WORKSPACE_PATH,
        },
    )

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

prozorro_dataform_transform_pipeline()