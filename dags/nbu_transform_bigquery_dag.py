from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from datetime import datetime

PROJECT_ID = "project-4deacada-3830-4d03-80c"
REGION = "us-central1"
REPOSITORY_ID = "post_stage"

@dag(dag_id="dataform_gcp_pipeline", start_date=datetime(2026, 9, 1), schedule=None)
def dataform_pipeline():

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main"
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

dataform_pipeline()
