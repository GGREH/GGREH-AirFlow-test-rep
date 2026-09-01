from datetime import datetime, timezone
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# Конфигурация GCP и BigQuery
GCP_PROJECT_ID = "project-4deacada-3830-4d03-80c"
GCP_CONN_ID = "google_cloud_default"
BQ_DATASET_ID = "nbu_data"
BQ_TABLE_ID = "exchange_rates"
NBU_API_BASE_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"


@dag(
    dag_id="nbu_exchange_rates_to_bigquery",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nbu", "rates", "bigquery", "api", "worker", "child"],
    description="Исполняющий DAG: выгружает курсы НБУ за переданный target_date и загружает в BigQuery",
)
def nbu_rates_pipeline():

    @task
    def extract_nbu_rates(**context) -> list[dict]:
        """
        Получение курсов валют из API НБУ за переданный из управляющего DAG target_date.
        """
        # Получаем target_date из конфигурации dag_run (переданной через TriggerDagRunOperator)
        dag_run_conf = context.get("dag_run").conf or {}
        target_date = dag_run_conf.get("target_date")

        if not target_date:
            raise ValueError("Параметр 'target_date' не был передан в dag_run.conf!")

        # Очищаем от кавычек, пробелов и дефисов: например, '"2026-08-20"' -> '20260820'
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

        print(f"Успешно получено записей о валютах: {len(data)}")
        return data

    @task
    def transform_rates(raw_data: list[dict]) -> list[dict]:
        """
        Преобразование данных: форматирование даты, приведение типов и добавление fetched_at.
        """
        if not raw_data:
            print("Нет данных для трансформации.")
            return []

        df = pd.DataFrame(raw_data)

        # Переименование колонок для читаемости
        df = df.rename(
            columns={
                "r030": "currency_id",
                "txt": "currency_name",
                "rate": "rate",
                "cc": "currency_code",
                "exchangedate": "exchange_date",
            }
        )

        # Преобразование даты курса (из формата DD.MM.YYYY в YYYY-MM-DD)
        df["exchange_date"] = pd.to_datetime(
            df["exchange_date"], format="%d.%m.%Y"
        ).dt.strftime("%Y-%m-%d")

        # Добавление времени выгрузки (UTC)
        current_ts = datetime.now(timezone.utc).isoformat()
        df["fetched_at"] = current_ts

        # Приведение типов
        df["currency_id"] = df["currency_id"].astype(int)
        df["rate"] = df["rate"].astype(float)
        df["currency_name"] = df["currency_name"].astype(str)
        df["currency_code"] = df["currency_code"].astype(str)

        print(f"Трансформировано {len(df)} записей. Пример:")
        print(df.head(3).to_string(index=False))

        return df.to_dict(orient="records")

    @task
    def load_to_bigquery(records: list[dict]) -> None:
        """
        Загрузка данных в таблицу BigQuery (с авто-созданием датасета/таблицы при необходимости).
        """
        if not records:
            print("Нет записей для загрузки в BigQuery.")
            return

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client: bigquery.Client = hook.get_client(project_id=GCP_PROJECT_ID)

        # Проверяем или создаем датасет
        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)

        table_ref = dataset_ref.table(BQ_TABLE_ID)

        # Определение схемы таблицы
        schema = [
            bigquery.SchemaField("currency_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("currency_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rate", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("currency_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("exchange_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        df = pd.DataFrame(records)
        df["exchange_date"] = pd.to_datetime(df["exchange_date"]).dt.date
        df["fetched_at"] = pd.to_datetime(df["fetched_at"])

        # Настройка задачи загрузки (добавление новых строк)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="exchange_date",
            ),
        )

        print(f"Загрузка {len(df)} строк в BigQuery `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"Успешно загружено {job.output_rows} строк в BigQuery!")

    # Граф выполнения задач
    raw_rates = extract_nbu_rates()
    transformed = transform_rates(raw_rates)
    load_to_bigquery(transformed)


nbu_rates_pipeline()
