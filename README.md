# Weather Data Pipeline with Airflow, DBT, and Dask

This project implements an automated data pipeline that fetches weather data, processes it using Dask for distributed computing, and transforms it using DBT for analytics-ready models. The pipeline is orchestrated using Apache Airflow running in Docker containers.

## Architecture

- **Apache Airflow**: Orchestrates the entire pipeline and manages task dependencies
- **Dask**: Handles distributed data processing for the weather data, tested as supplement/replacement to pandas
- **DBT**: Transforms raw weather data into analytics-ready models
- **PostgreSQL**: Stores both raw and transformed data
- **Docker**: Containerizes all components for consistent deployment

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- dbt Core & dbt-postgres
- Access to Open-meteo (free)

## Setup
 1. Install Docker and Docker Compose
 2. run ` docker-compose up -d `
 3. go to http://localhost:8080 to view the Airflow control plane
 4. use "airflow" as both the username and password
 5. You're in! now you can use my dag and add your own


## Project Structure
- #### DAGs
    - **weather_dag**: an orchestrated series of tasks which retreives data from Open-meteo
- #### dbt
    - **weather_model**: a view model which converts some of the units to test the functioning of dbt

    

