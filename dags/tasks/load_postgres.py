import os

import dask.dataframe as dd
import pandas as pd
import psycopg2
import json
from dotenv import load_dotenv
from psycopg2 import sql
from airflow.decorators import task

load_dotenv(override=True)


def load_data_to_postgres(**kwargs):
    """Load weather data into PostgreSQL."""
    
    print(kwargs)
    ti = kwargs["ti"]

    json_data = ti.xcom_pull(task_ids="fetch_weather", key="weather_data")
    weather_df = pd.read_json(json_data)
    dask_df = dd.from_pandas(weather_df, npartitions=1)

    db_username = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    cursor = None  
    conn = None    

    try:
        
        conn = psycopg2.connect(
            host=db_host,
            database="postgres", 
            user=db_username,
            password=db_password
        )
        
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()  
        
        
        table_statement = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            observed_at TIMESTAMP,
            temperature FLOAT,
            apparent_temperature FLOAT,
            wind_speed FLOAT,
            visibility FLOAT
        );
        """
        cursor.execute(table_statement)
        conn.commit() 

        
        for partition in dask_df.to_delayed():
            records = partition.compute().to_dict(orient='records')  
            insert_query = """
                INSERT INTO weather_data (observed_at, temperature, apparent_temperature, wind_speed, visibility)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.executemany(insert_query, [(record['observed_at'], record['temperature'], 
                                                record['apparent_temperature'], record['wind_speed'], 
                                                record['visibility']) for record in records])
        
        conn.commit()  # Commit the inserts
        print("Data loaded successfully to PostgreSQL.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:  
            cursor.close()
        if conn:  
            conn.close()
    

