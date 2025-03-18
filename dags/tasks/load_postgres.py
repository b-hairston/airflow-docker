import os

import dask.dataframe as dd
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from airflow.decorators import task

load_dotenv(override=True)


@task
def load_data_to_postgres(weather_data: list, **kwargs):
    """Load weather data into PostgreSQL."""
    # Create DataFrame from weather data
    df = pd.DataFrame(weather_data)
    dask_df = dd.from_pandas(df, npartitions=1)

    # Get database credentials from environment variables
    db_username = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    cursor = None  # Initialize cursor
    conn = None    # Initialize connection

    try:
        # Connect to PostgreSQL server (default database)
        conn = psycopg2.connect(
            host=db_host,
            database="postgres",  # Connect to the default database to create the new one
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
        
        # Create table if it does not exist
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
        conn.commit()  # Commit the table creation

        # Insert data into the weather_data table
        for partition in dask_df.to_delayed():
            records = partition.compute().to_dict(orient='records')  
            # Using `executemany` for batch inserts can be more efficient
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
        if cursor:  # Check if cursor was created before trying to close it
            cursor.close()
        if conn:  # Check if connection was created before trying to close it
            conn.close()
    

