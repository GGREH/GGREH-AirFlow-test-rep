from datetime import datetime
import pandas as pd
import pyarrow as pa
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

GCP_PROJECT_ID = "project-4deacada-3830-4d03-80c"
GCP_CONN_ID = "google_cloud_default"

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
        Запрос к BigQuery: выгружаем 1 из локальной таблицы
        """
        print(f"Подключение к BigQuery через '{GCP_CONN_ID}'...")
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client(project_id=GCP_PROJECT_ID)

        sql_query = """
            SELECT *
            FROM `project-4deacada-3830-4d03-80c.post_stage.second_view`
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
        print("Записи из таблицы")
        print("=" * 60)
        print(df.to_string(index=False))
        print("=" * 60)

    data = query_bigquery()
    analyze_and_display(data)

bigquery_pipeline()
