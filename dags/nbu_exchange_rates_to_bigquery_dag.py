from datetime import datetime, timezone
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.models.param import Param
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
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["nbu", "rates", "bigquery", "api", "hourly", "parametrized"],
    description="Выгрузка курсов валют НБУ за указанную дату и загрузка в BigQuery",
    params={
        "target_date": Param(
            default="",
            type=["null", "string"],
            description="Дата для выгрузки в формате YYYY-MM-DD или YYYYMMDD (например, 2026-09-01). Оставьте пустым для использования текущей даты/даты запуска.",
        )
    },
)
def nbu_rates_pipeline():

    @task
    def extract_nbu_rates(**context) -> list[dict]:
        """
        Получение курсов валют из API НБУ за переданный или рассчитанный день.
        """
        # 1. Получаем параметр из ручного запуска (Trigger with config)
        param_date = context.get("params", {}).get("target_date")

        if param_date:
            # Очищаем от дефисов, если передали YYYY-MM-DD -> YYYYMMDD
            clean_date = str(param_date).replace("-", "").strip()
            date_query = clean_date
            print(f"Используется переданная дата из параметров: {param_date} (в API: {date_query})")
        else:
            # Если параметр не задан, берем дату запуска Airflow (ds_nodash -> YYYYMMDD)
            date_query = context.get("ds_nodash") or datetime.now(timezone.utc).strftime("%Y%m%d")
            print(f"Параметр не задан, используется дата запуска: {date_query}")

        # Формируем запрос с параметром даты
        query_params = {
            "date": date_query,
            "json": "",
        }

        print(f"Отправка запроса к API НБУ: {NBU_API_BASE_URL} с параметрами {query_params}")
        response = requests.get(NBU_API_BASE_URL, params=query_params, timeout=30)
        response.raise_for_status()

        data = response.json()
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
