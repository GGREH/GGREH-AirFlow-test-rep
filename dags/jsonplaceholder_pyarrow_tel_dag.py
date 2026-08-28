import json
import urllib.request
from datetime import datetime
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, text
from airflow.decorators import dag, task

# Параметры подключения к созданной БД PostgreSQL внутри Kubernetes
DB_CONN_STR = "postgresql+psycopg2://analytics_user:analytics_password@target-postgres:5432/analytics_db"

@dag(
    dag_id="jsonplaceholder_pyarrow_etl_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "pandas", "pyarrow", "postgres", "jsonplaceholder"],
    doc_md="""
    ### Расширенный ETL Pipeline (PyArrow + Pandas):
    1. **Extract**: Загрузка сырых данных с REST API JSONPlaceholder.
    2. **Transform (PyArrow + Pandas)**:
       - Строгая типизация схемы через `pyarrow.Table`.
       - Преобразование в `pandas.DataFrame`.
       - Feature Engineering: расчет длины заголовков, количества слов, категоризация по объему (`short`/`medium`/`long`).
       - Очистка текста и дедупликация.
    3. **Load**: Загрузка обработанного датасета в PostgreSQL (`posts_analytics`).
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
        print(f"Извлечено сырых записей: {len(data)}")
        return data

    @task
    def transform_with_pyarrow_and_pandas(raw_data: list[dict]) -> list[dict]:
        """
        [T]ransform: Продвинутая обработка данных с помощью PyArrow и Pandas.
        """
        if not raw_data:
            print("Нет данных для обработки.")
            return []

        # 1. Валидация типов данных через PyArrow Schema
        arrow_schema = pa.schema([
            ("userId", pa.int64()),
            ("id", pa.int64()),
            ("title", pa.string()),
            ("body", pa.string())
        ])
        arrow_table = pa.Table.from_pylist(raw_data, schema=arrow_schema)
        print(f"PyArrow Schema валидирована:\n{arrow_table.schema}")

        # 2. Конвертация PyArrow Table в Pandas DataFrame
        df = arrow_table.to_pandas()

        # 3. Переименование колонок в snake_case
        df = df.rename(columns={"userId": "user_id", "id": "post_id"})

        # 4. Очистка текстовых полей
        df["title"] = df["title"].fillna("").astype(str).str.strip().str.capitalize()
        df["body"] = df["body"].fillna("").astype(str).str.replace("\n", " ", regex=False).str.strip()

        # 5. Feature Engineering: добавление метрик и аналитических колонок
        df["title_length"] = df["title"].str.len()
        df["body_word_count"] = df["body"].apply(lambda text_val: len(text_val.split()) if text_val else 0)

        # Категоризация постов по количеству слов: short (<20), medium (20-35), long (>35)
        def categorize_length(word_count: int) -> str:
            if word_count < 20:
                return "short"
            elif word_count <= 35:
                return "medium"
            return "long"

        df["category"] = df["body_word_count"].apply(categorize_length)
        df["processed_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # 6. Дедупликация и сортировка
        df = df.drop_duplicates(subset=["post_id"])
        df = df.sort_values(by=["user_id", "post_id"])

        print(f"Статистика обработанных данных:")
        print(f"- Всего строк: {len(df)}")
        print(f"- Распределение категорий:\n{df['category'].value_counts().to_string()}")
        print(f"- Среднее количество слов: {df['body_word_count'].mean():.2f}")

        # Возвращаем список словарей для передачи в таску загрузки
        return df.to_dict(orient="records")

    @task
    def load_to_postgres(records: list[dict]) -> int:
        """
        [L]oad: Создание аналитической таблицы и вставка обогащенных данных в PostgreSQL.
        """
        if not records:
            print("Нет данных для загрузки.")
            return 0

        engine = create_engine(DB_CONN_STR)

        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS posts_analytics (
                user_id INT,
                post_id INT PRIMARY KEY,
                title VARCHAR(255),
                body TEXT,
                title_length INT,
                body_word_count INT,
                category VARCHAR(20),
                processed_at TIMESTAMP
            );
        """)

        insert_query = text("""
            INSERT INTO posts_analytics (
                user_id, post_id, title, body, 
                title_length, body_word_count, category, processed_at
            )
            VALUES (
                :user_id, :post_id, :title, :body, 
                :title_length, :body_word_count, :category, :processed_at
            )
            ON CONFLICT (post_id) DO UPDATE SET
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                title_length = EXCLUDED.title_length,
                body_word_count = EXCLUDED.body_word_count,
                category = EXCLUDED.category,
                processed_at = EXCLUDED.processed_at;
        """)

        # 1. Создание таблицы с autocommit (безопасно при повторных запусках)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            try:
                conn.execute(create_table_query)
            except Exception as e:
                print(f"Таблица уже создана: {e}")

        # 2. Вставка данных в транзакции
        with engine.begin() as conn:
            conn.execute(insert_query, records)

        print(f"Успешно загружено {len(records)} аналитических строк в таблицу 'posts_analytics'.")
        return len(records)

    # Связывание этапов ETL пайплайна:
    raw_posts = extract_from_api()
    transformed_posts = transform_with_pyarrow_and_pandas(raw_posts)
    load_to_postgres(transformed_posts)

# Экземпляр DAG-а
jsonplaceholder_pyarrow_pipeline()
