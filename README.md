# Weather Data Pipeline with Airflow, DBT, and Dask

This project implements an automated data pipeline that fetches weather data, processes it using Dask for distributed computing, and transforms it using DBT for analytics-ready models. The pipeline is orchestrated using Apache Airflow running in Docker containers.

## Architecture

- **Apache Airflow**: Orchestrates the entire pipeline and manages task dependencies
- **Dask**: Handles distributed data processing for large weather datasets
- **DBT**: Transforms raw weather data into analytics-ready models
- **PostgreSQL**: Stores both raw and transformed data
- **Docker**: Containerizes all components for consistent deployment

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- dbt
- Access to weather API (configured in environment variables)

## Project Structure
- #### DAGs

- #### dbt


    

