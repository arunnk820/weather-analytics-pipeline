from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    'weather_analytics_pipeline',
    start_date=datetime(2026, 4, 2),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    pass # logic to be added in Day 2
