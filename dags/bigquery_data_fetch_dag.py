from datetime import datetime
import pandas as pd
import pyarrow as pa
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

GCP_PROJECT_ID = "project-4deacada-3830-4d03-80c"
GCP_CONN_ID = "bigquery"

@dag(
    dag_id="bigquery_fetch_data_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gcp", "bigquery", "pyarrow", "pandas"]
)
def bigquery_pipeline():

    @task
    def query_bigquery() -> list[dict]:
        """
        Запрос к BigQuery: выгружаем топ популярных имен из открытого датасета Google
        """
        print(f"Подключение к BigQuery через '{GCP_CONN_ID}'...")
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client(project_id=GCP_PROJECT_ID)

        sql_query = """
            SELECT 
                name,
                gender,
                SUM(number) AS total_count
            FROM `bigquery-public-data.usa_names.usa_1910_2013`
            WHERE year >= 2000
            GROUP BY name, gender
            ORDER BY total_count DESC
            LIMIT 20;
        """
        
        print("Выполнение запроса в BigQuery...")
        query_job = client.query(sql_query)
        
        # Напрямую получаем PyArrow Table из BigQuery
        arrow_table = query_job.to_arrow()
        print(f"PyArrow Schema:\n{arrow_table.schema}")
        print(f"Строк получено: {arrow_table.num_rows}")

        df: pd.DataFrame = arrow_table.to_pandas()
        return df.to_dict(orient="records")

    @task
    def analyze_and_display(rows: list[dict]) -> None:
        if not rows:
            print("Данные не получены.")
            return

        df = pd.DataFrame(rows)
        print("=" * 60)
        print("ТОП-20 ИМЕН ИЗ BIGQUERY:")
        print("=" * 60)
        print(df.to_string(index=False))
        print("=" * 60)
        print(f"Всего людей в выборке: {df['total_count'].sum():,}")

    data = query_bigquery()
    analyze_and_display(data)

bigquery_pipeline()
