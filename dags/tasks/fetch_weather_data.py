import os

import duckdb
import httpx
import pandas as pd
from pydantic import BaseModel
from airflow.decorators import task
from tasks.models import WeatherData, validate_data


@task
def fetch_weather_data(db_path="weather_data.duckdb", table_name="weather_cache", **kwargs) -> pd.DataFrame:
    base_url = "https://api.open-meteo.com/v1/forecast"
    
    # Define query parameters in a dictionary
    params = {
        "latitude": 41.85,
        "longitude": -87.65,
        "hourly": "temperature_2m,apparent_temperature,wind_speed_10m,visibility"
    }

    # Make the GET request with parameters
    if not os.path.exists(db_path):
        response = httpx.get(base_url, params=params)    
        response.raise_for_status()
    
        data = response.json()
        time_df = pd.DataFrame(data["hourly"]["time"])
        temp_df = pd.DataFrame(data["hourly"]["temperature_2m"])
        apparent_temp_df = pd.DataFrame(data["hourly"]["apparent_temperature"])
        wind_speed_df = pd.DataFrame(data["hourly"]["wind_speed_10m"])
        visibility_df = pd.DataFrame(data["hourly"]["visibility"])
        
        # Combine data into a single DataFrame
        df = pd.concat([time_df, temp_df, apparent_temp_df, wind_speed_df, visibility_df], axis=1)
        df.columns = ["observed_at", "temperature", "apparent_temperature", "wind_speed", "visibility"]
        
        # Validate the data (assuming validate_data is defined)
        validated_data = validate_data(df, WeatherData)
        validated_df = pd.DataFrame(validated_data)

        # Load data into DuckDB
        # Connect to DuckDB (create if not exists)
        conn = duckdb.connect(db_path)
        
        # Create the table if it does not exist
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            observed_at TIMESTAMP,
            temperature FLOAT,
            apparent_temperature FLOAT,
            wind_speed FLOAT,
            visibility FLOAT
        )
        """)

        # Insert the new data into DuckDB
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    else:
        conn = duckdb.connect(db_path)
        validated_df = conn.execute(f"SELECT * FROM {table_name}").fetch_df()
    

    # Close the DuckDB connection
    conn.close()

    return validated_df






