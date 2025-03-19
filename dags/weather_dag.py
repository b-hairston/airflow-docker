import os
import sys

import dask.dataframe as dd
import duckdb
import httpx
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from tasks.models import WeatherData, validate_data


@dag(
    dag_id="weather_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)
def weather_dag():

    @task
    def fetch_weather_data_task(db_path="weather_data.duckdb", table_name="weather_cache") -> list:
        """Fetch weather data, validate, and return as a JSON-serializable object."""
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 41.85,
            "longitude": -87.65,
            "hourly": "temperature_2m,apparent_temperature,wind_speed_10m,visibility"
        }

        if not os.path.exists(db_path):
            response = httpx.get(base_url, params=params)    
            response.raise_for_status()
            data = response.json()

            time_df = pd.DataFrame(data["hourly"]["time"])
            temp_df = pd.DataFrame(data["hourly"]["temperature_2m"])
            apparent_temp_df = pd.DataFrame(data["hourly"]["apparent_temperature"])
            wind_speed_df = pd.DataFrame(data["hourly"]["wind_speed_10m"])
            visibility_df = pd.DataFrame(data["hourly"]["visibility"])

            df = pd.concat([time_df, temp_df, apparent_temp_df, wind_speed_df, visibility_df], axis=1)
            df.columns = ["observed_at", "temperature", "apparent_temperature", "wind_speed", "visibility"]
            
            validated_data = validate_data(df, WeatherData)
            validated_df = pd.DataFrame(validated_data)
            validated_df["observed_at"] = validated_df["observed_at"].astype(str)

            conn = duckdb.connect(db_path)
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    observed_at VARCHAR,
                    temperature FLOAT,
                    apparent_temperature FLOAT,
                    wind_speed FLOAT,
                    visibility FLOAT
                )
            """)

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            conn.close()
        else:
            conn = duckdb.connect(db_path)
            validated_df = conn.execute(f"SELECT * FROM {table_name}").fetch_df()
            conn.close()

        return validated_df.to_dict(orient="records")  # Return a serializable list of dictionaries

    @task
    def load_data_to_postgres_task(weather_data: list):
        """Load weather data into PostgreSQL."""
        if not weather_data:
            raise ValueError("No data received from fetch_weather_data_task")

        weather_df = pd.DataFrame(weather_data)  # Convert back to DataFrame
        dask_df = dd.from_pandas(weather_df, npartitions=1)

        db_username = "postgres_user"
        db_password = "postgres"
        db_host = "airflow-docker-analytics-db-1"
        db_port = 5432
        db_name = "postgres"
        cursor = None  
        conn = None    

        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_username,
                password=db_password,
                host=db_host,
                port=db_port
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    observed_at TIMESTAMP,
                    temperature FLOAT,
                    apparent_temperature FLOAT,
                    wind_speed FLOAT,
                    visibility FLOAT
                );
            """)
            conn.commit()

            for partition in dask_df.to_delayed():
                records = partition.compute().to_dict(orient='records')
                insert_query = """
                    INSERT INTO weather_data (observed_at, temperature, apparent_temperature, wind_speed, visibility)
                    VALUES (%s, %s, %s, %s, %s);
                """
                cursor.executemany(insert_query, [(r['observed_at'], r['temperature'], 
                                                    r['apparent_temperature'], r['wind_speed'], 
                                                    r['visibility']) for r in records])
            
            conn.commit()
            print("Data loaded successfully to PostgreSQL.")

        except Exception as e:
            print(f"Error: {e}")
        finally:
            cursor.close()
            conn.close()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --models weather_model --profiles-dir /opt/airflow/dags/dbt/fuckin_dbt",
        cwd="/opt/airflow/dags/dbt/fuckin_dbt",
        # bash_command="cd /opt/airflow/dags/dbt/dask_airflow && dbt debug"
    )

    # Define task dependencies
    weather_data = fetch_weather_data_task()
    load_data_to_postgres_task(weather_data) >> dbt_run

weather_dag()
