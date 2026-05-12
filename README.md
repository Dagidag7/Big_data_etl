# Big Data ETL Pipeline Project

## Project Overview
This project implements an ETL pipeline using PySpark, DuckDB, and Prefect for airline data analytics.

## Technologies Used
- Python
- PySpark
- DuckDB
- Prefect
- Parquet

## Data Sources
- flights.csv
- airports.json
- parquet file

## Architecture
CSV/JSON → PySpark → Parquet → DuckDB

## How to Run

```bash
python orchestration/pipeline_flow.py
```

## Features
- Data Extraction
- Data Transformation
- Data Loading
- Workflow Orchestration
- Parquet Storage