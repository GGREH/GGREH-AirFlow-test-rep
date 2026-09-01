FROM apache/airflow:3.2.2

RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    apache-airflow-providers-git \
    google-cloud-bigquery \
    google-cloud-bigquery-storage \
    db-dtypes \
    pyarrow \
    pandas \
    psycopg2-binary \
    sqlalchemy \

    COPY ./dags /opt/airflow/dags/
