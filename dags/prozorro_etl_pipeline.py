from datetime import datetime, timezone
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

GCP_PROJECT_ID = "project-4deacada-3830-4d03-80c"
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET_ID = "prozorro_staging"
BQ_TABLE_ID = "tmp_prozorro_tenders_data"
PROZORRO_API_BASE_URL = "https://public.api.openprocurement.org/api/2.5/tenders"

@dag(
    dag_id="prozorro_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 9, 1, 11, 0, 0),
    catchup=False,
    tags=["prozorro", "staging", "tmp", "raw", "bigquery", "api", "worker", "child"],
    description="Исполняющий DAG: выгружает сырые данные тендеров ProZorro во временную таблицу prozorro_staging.tmp_prozorro_tenders_data",
)
def prozorro_etl_pipeline():
    
    @task
    def extract_prozorro_tenders(**context) -> list[dict]:
        """
        Получение сырых данных тендеров из открытого API ProZorro за target_date.
        """
        dag_run_conf = context.get("dag_run").conf or {}
        target_date = dag_run_conf.get("target_date")

        if not target_date:
            raise ValueError("Параметр 'target_date' не был передан в dag_run.conf!")

        target_date_str = str(target_date).strip("\"' \t\r\n")

        query_params = {
            "descending": 1,
            "limit": 100,
        }

        print(f"Получен target_date: {repr(target_date_str)}")
        print(f"Отправка запроса к API ProZorro: {PROZORRO_API_BASE_URL} с параметрами {query_params}")

        response = requests.get(PROZORRO_API_BASE_URL, params=query_params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        raw_items = payload.get("data", [])

        print(f"Успешно получено сырых записей тендеров: {len(raw_items)}")
        return raw_items
    @task
    def load_raw_to_bigquery(raw_data: list[dict], **context) -> None:
        """
        Прямая загрузка сырых данных во временную таблицу prozorro_staging.tmp_prozorro_tenders_data.
        Инкрементальный merge в prozorro_staging.stg_prozorro_tenders выполняется Dataform.
        """

        if not raw_data:
            print("Нет сырых записей для загрузки в BigQuery staging.")
            return
        
        dag_run_conf = context.get("dag_run").conf or {}
        target_date_val = str(dag_run_conf.get("target_date", "")).strip("\"' \t\r\n")

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client: bigquery.Client = hook.get_client(project_id=GCP_PROJECT_ID)

        for ds_id in ["prozorro_staging", "prozorro_core", "prozorro_reporting"]:
            dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, ds_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "us-central1"
            client.create_dataset(dataset, exists_ok=True)

        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET_ID)
        table_ref = dataset_ref.table(BQ_TABLE_ID)

        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="ID тендера в ProZorro"),
            bigquery.SchemaField("dateModified", "STRING", mode="NULLABLE", description="Дата изменения тендера"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED", description="Время загрузки во временную таблицу"),
            bigquery.SchemaField("target_date", "STRING", mode="NULLABLE", description="Целевая дата запроса"),
        ]

        df = pd.DataFrame(raw_data)

        if "id" not in df.columns:
            df["id"] = None
        if "dateModified" not in df.columns:
            df["dateModified"] = None

        df = df[["id", "dateModified"]].copy()

        current_ts = datetime.now(timezone.utc)
        df["loaded_at"] = current_ts
        df["target_date"] = target_date_val

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        print(f"Загрузка {len(df)} сырых строк в `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"Успешно загружено {job.output_rows} строк в BigQuery `{BQ_DATASET_ID}.{BQ_TABLE_ID}`!")

    raw_tenders = extract_prozorro_tenders()
    load_raw_to_bigquery(raw_tenders)


prozorro_etl_pipeline()