from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
from airflow import DAG
from tasks.fetch_weather_data import fetch_weather_data
from tasks.load_postgres import load_data_to_postgres


with DAG(
    dag_id="weather_dag",
    schedule_interval="@daily", 
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data,
        provide_context=True, 
    )
    
    load_data_to_postgres_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres,
        provide_context=True,  
    )
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/dbt/dask_airflow && dbt run --select weather_model --profiles-dir /opt/airflow/dags/dbt/dask_airflow"    )
    
    fetch_task >> load_data_to_postgres_task >> dbt_run
    
    
    
