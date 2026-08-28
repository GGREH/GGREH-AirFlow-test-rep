import json
import urllib.request
from datetime import datetime
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import create_engine, text
from airflow.decorators import dag, task

# Параметры подключения к созданной БД PostgreSQL внутри Kubernetes
DB_CONN_STR = "postgresql+psycopg2://analytics_user:analytics_password@target-postgres:5432/analytics_db"

@dag(
    dag_id="jsonplaceholder_pyarrow_etl_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "pyarrow", "postgres", "jsonplaceholder"],
    doc_md="""
    ### ETL Pipeline с использованием PyArrow:
    1. **Extract**: загрузка JSON данных с REST API.
    2. **Transform (PyArrow)**: типизация схемы через `pyarrow.Table`, очистка и обогащение метаданными.
    3. **Load**: создание таблицы и вставка данных в PostgreSQL.
    """
)
def jsonplaceholder_pyarrow_pipeline():

    @task
    def extract_from_api() -> list[dict]:
        """
        [E]xtract: Получение постов из внешнего REST API.
        """
        url = "https://jsonplaceholder.typicode.com/posts"
        print(f"Запрос данных с {url}...")
        req = urllib.request.Request(url, headers={"User-Agent": "Airflow-ETL"})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        print(f"Извлечено записей: {len(data)}")
        return data

    @task
    def transform_with_pyarrow(raw_data: list[dict]) -> list[dict]:
        """
        [T]ransform: Обработка с использованием библиотеки PyArrow.
        Создаем строго типизированную Apache Arrow таблицу, трансформируем поля и валидируем.
        """
        # 1. Задаем строгую схему данных через PyArrow Schema
        schema = pa.schema([
            ("userId", pa.int64()),
            ("id", pa.int64()),
            ("title", pa.string()),
            ("body", pa.string())
        ])

        # 2. Создаем Arrow RecordBatch / Table
        table = pa.Table.from_pylist(raw_data, schema=schema)
        print(f"PyArrow Schema:\n{table.schema}")
        print(f"Всего строк в Arrow Table: {table.num_rows}")

        # 3. Трансформация: добавляем метку времени загрузки и очищаем переносы строк
        transformed_records = []
        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for row in table.to_pylist():
            transformed_records.append({
                "user_id": row["userId"],
                "post_id": row["id"],
                "title": row["title"].strip() if row["title"] else "",
                "body": row["body"].replace("\n", " ").strip() if row["body"] else "",
                "processed_at": now_ts
            })

        print(f"Успешно обработано строк с помощью PyArrow: {len(transformed_records)}")
        return transformed_records

    @task
    def load_to_postgres(records: list[dict]) -> int:
        """
        [L]oad: Создание таблицы и сохранение подготовленных данных в БД.
        """
        if not records:
            print("Нет данных для загрузки.")
            return 0

        engine = create_engine(DB_CONN_STR)

        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS posts (
                user_id INT,
                post_id INT PRIMARY KEY,
                title VARCHAR(255),
                body TEXT,
                processed_at TIMESTAMP
            );
        """)

        insert_query = text("""
            INSERT INTO posts (user_id, post_id, title, body, processed_at)
            VALUES (:user_id, :post_id, :title, :body, :processed_at)
            ON CONFLICT (post_id) DO UPDATE SET
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                processed_at = EXCLUDED.processed_at;
        """)

        # Выполняем CREATE TABLE с обработкой уже существующей таблицы
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            try:
                conn.execute(create_table_query)
            except Exception as e:
                print(f"Таблица уже создана: {e}")

        # Выполняем вставку данных в транзакции
        with engine.begin() as conn:
            conn.execute(insert_query, records)

        print(f"Успешно загружено/обновлено {len(records)} строк в таблице 'posts'.")
        return len(records)

    # Связывание этапов пайплайна:
    raw_posts = extract_from_api()
    arrow_transformed = transform_with_pyarrow(raw_posts)
    load_to_postgres(arrow_transformed)

# Экземпляр DAG-а
jsonplaceholder_pyarrow_pipeline()
