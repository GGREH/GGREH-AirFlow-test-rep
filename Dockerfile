FROM apache/airflow:latest

RUN pip install apache-airflow-providers-git --no-cache-dir pyarrow psycopg2-binary sqlalchemy

COPY ./dags /opt/airflow/dags/
