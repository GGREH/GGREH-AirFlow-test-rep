from datetime import datetime, timezone
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# Конфигурация GCP и BigQuery (Staging слой: содержит tmp_nbu_kurs_data и stg_nbu_kurs)
GCP_PROJECT_ID = "project-4deacada-3830-4d03-80c"
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET_ID = "nbu_staging"
BQ_TABLE_ID = "tmp_nbu_kurs_data"
NBU_API_BASE_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"


@dag(
    dag_id="nbu_exchange_rates_to_bigquery",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nbu", "staging", "tmp", "raw", "bigquery", "api", "worker", "child"],
    description="Исполняющий DAG: выгружает сырые курсы НБУ за target_date во временную таблицу nbu_staging.tmp_nbu_kurs_data",
)
def nbu_rates_pipeline():

    @task
    def extract_nbu_rates(**context) -> list[dict]:
        """
        Получение сырых курсов валют из API НБУ за переданный target_date.
        """
        dag_run_conf = context.get("dag_run").conf or {}
        target_date = dag_run_conf.get("target_date")

        if not target_date:
            raise ValueError("Параметр 'target_date' не был передан в dag_run.conf!")

        clean_date = str(target_date).strip("\"' \t\r\n").replace("-", "").strip()

        query_params = {
            "date": clean_date,
            "json": "",
        }

        print(f"Получен target_date: {repr(target_date)} (для API НБУ: {repr(clean_date)})")
        print(f"Отправка запроса к API НБУ: {NBU_API_BASE_URL} с параметрами {query_params}")

        response = requests.get(NBU_API_BASE_URL, params=query_params, timeout=30)
        response.raise_for_status()

        try:
            data = response.json()
        except Exception as err:
            print(f"Ошибка парсинга JSON. Ответ API НБУ (первые 300 символов):\n{response.text[:300]}")
            raise err

        print(f"Успешно получено сырых записей: {len(data)}")
        return data

    @task
    def load_raw_to_bigquery(raw_data: list[dict], **context) -> None:
        """
        Прямая загрузка сырых данных во временную таблицу nbu_staging.tmp_nbu_kurs_data.
        Инкрементальный merge в nbu_staging.stg_nbu_kurs выполняется Dataform.
        """
        if not raw_data:
            print("Нет сырых записей для загрузки в BigQuery staging.")
            return

        dag_run_conf = context.get("dag_run").conf or {}
        target_date_val = str(dag_run_conf.get("target_date", "")).strip("\"' \t\r\n")

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client: bigquery.Client = hook.get_client(project_id=GCP_PROJECT_ID)

        # Проверяем и создаем 3 датасета DWH (staging, core, reporting)
        for ds_id in ["nbu_staging", "nbu_core", "nbu_reporting"]:
            dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, ds_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "us-central1"
            client.create_dataset(dataset, exists_ok=True)

        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET_ID)
        table_ref = dataset_ref.table(BQ_TABLE_ID)

        # Схема сырой временной таблицы
        schema = [
            bigquery.SchemaField("r030", "INTEGER", mode="NULLABLE", description="Код валюты из API НБУ"),
            bigquery.SchemaField("txt", "STRING", mode="NULLABLE", description="Название валюты"),
            bigquery.SchemaField("rate", "FLOAT", mode="REQUIRED", description="Курс валюты"),
            bigquery.SchemaField("cc", "STRING", mode="REQUIRED", description="Буквенный код валюты (USD, EUR...)"),
            bigquery.SchemaField("exchangedate", "STRING", mode="REQUIRED", description="Дата курса в формате API (DD.MM.YYYY)"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED", description="Время загрузки во временную таблицу"),
            bigquery.SchemaField("target_date", "STRING", mode="NULLABLE", description="Целевая дата запроса"),
        ]

        df = pd.DataFrame(raw_data)

        # Добавляем технические метаданные загрузки
        current_ts = datetime.now(timezone.utc)
        df["loaded_at"] = current_ts
        df["target_date"] = target_date_val

        # Временная таблица перезаписывается на каждый прогон выгрузки
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        print(f"Загрузка {len(df)} сырых строк в `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"Успешно загружено {job.output_rows} строк в BigQuery `{BQ_DATASET_ID}.{BQ_TABLE_ID}`!")

    raw_rates = extract_nbu_rates()
    load_raw_to_bigquery(raw_rates)


nbu_rates_pipeline()
