FROM apache/airflow:latest

RUN pip install apache-airflow-providers-git

COPY ./dags /opt/airflow/dags/
