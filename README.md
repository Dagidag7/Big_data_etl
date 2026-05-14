# Big Data ETL Pipeline Project

## Team Members

| No. | Name | Student ID |
|-----|------|------------|
| 1 | BISRAT ENDALE | DBU1401063 |
| 2 | DAGIMAWIT KELEM | DBU1501101 |
| 3 | ZELEKE MEKONNEN | DBU1501598 |
| 4 | FIKRE TIBEBU | DBUR/0737/13 |
| 5 | ASHENAFI ABERA | DBU1403014 |
| 6 | FITALEW ABATE | DBU1501210 |
| 7 | ELEFACHEW FETENE | DBU1501148 |
| 8 | RAHEL MEKONEN | DBU1501427 |
| 9 | FIREHIWOT TESFAYE | DBU1501207 |

## Project Overview

This project implements an ETL pipeline using PySpark, DuckDB, and Prefect for airline data analytics. The pipeline extracts flight and airport data, transforms it to derive delay status information, and loads it into efficient storage formats for analysis.

## Objectives

- **Data Extraction**: Read flight data from CSV and airport data from JSON
- **Data Transformation**: Clean and process data to identify delayed flights
- **Data Loading**: Store processed data in Parquet format and DuckDB
- **Workflow Orchestration**: Automate the entire pipeline with Prefect
- **Performance**: Utilize PySpark for distributed data processing

## Technologies Used

- Python
- PySpark
- DuckDB
- Prefect
- Parquet

## Architecture Diagram

[ETL Pipeline Architecture](Architecture_diagram.png)

## ETL Workflow

### 1. Data Preparation (`scripts/convert_airports.py`)

Converts the original nested `airports.json` into JSON Lines format (`airports_clean.json`) for easier processing with PySpark. It adds an `airport_code` field to each airport entry.

### 2. Extract Phase (`scripts/extract_data.py`)

**Spark Configuration**:
- Master: local[*]
- Driver memory: 4g
- Executor memory: 4g
- Shuffle partitions: 4

**Steps**:
1. Reads `data/flights.csv` with header and inferred schema
2. Reads `data/airports_clean.json`
3. Cleans flight data:
   - Drops rows with missing values in `FL_DATE`, `AIRLINE`, `ORIGIN`, `DEST`
   - Fills null `DEP_DELAY` and `ARR_DELAY` values with 0
   - Creates `DELAY_STATUS` column (1 if arrival delay > 15 minutes, else 0)
4. Saves cleaned data to:
   - `output/flights_cleaned` (Parquet)
   - `output/airports_cleaned` (Parquet)

### 3. Transform Phase (`scripts/transform_data.py`)

**Steps**:
1. Reads flights data from `data/flights.csv` and airports from `data/airports_clean.json`
2. Cleans data:
   - Drops rows with null `ARR_DELAY`
   - Filters out cancelled flights (`CANCELLED == 0`)
3. Creates `delay_status` column with string values:
   - "Delayed" if `ARR_DELAY > 15`
   - "On Time" otherwise
4. Selects final columns: `FL_DATE`, `AIRLINE`, `ORIGIN`, `DEST`, `ARR_DELAY`, `delay_status`

### 4. Load Phase (`scripts/load_data.py`)

**Steps**:
1. Reads flights data from `data/flights.csv`
2. Performs inline transformation:
   - Selects `FL_DATE`, `AIRLINE`, `ORIGIN`, `DEST`, `ARR_DELAY`
   - Creates `delay_status` column
3. Saves transformed data as Parquet to `output/flights_parquet`
4. Loads Parquet data into DuckDB database at `output/flights.duckdb`
5. Verifies data by querying DuckDB and displaying sample records

### 5. Orchestration (`orchestration/pipeline_flow.py`)

Uses Prefect to define and run the complete ETL pipeline:
- **Tasks**:
  - `extract()`: Executes `scripts/extract_data.py`
  - `transform()`: Executes `scripts/transform_data.py`
  - `load()`: Executes `scripts/load_data.py`
- **Flow**: `etl_pipeline()` runs tasks sequentially

## Installation Guide

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for Spark)
- 4GB+ RAM recommended

### Setup Steps

1. **Clone or Download the Project**
```bash
cd Big_data_etl-main
```

2. **Create Virtual Environment**
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# Or on Linux/Mac: source venv/bin/activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

4. **Prepare Data**
- Place `flights.csv` in the `data/` directory
- Ensure `airports.json` is present (already included)
- Convert airports JSON (optional but recommended):
```bash
python scripts/convert_airports.py
```

## How to Run the Pipeline

### Full Pipeline (Recommended)
```bash
python orchestration/pipeline_flow.py
```

### Individual Scripts
```bash
# Extract Phase
python scripts/extract_data.py

# Transform Phase
python scripts/transform_data.py

# Load Phase
python scripts/load_data.py
```

## Dashboard / BI Report

### Data Available in DuckDB

The final table `flights` in DuckDB contains:
- `FL_DATE`: Flight date
- `AIRLINE`: Airline code
- `ORIGIN`: Origin airport code
- `DEST`: Destination airport code
- `ARR_DELAY`: Arrival delay in minutes
- `delay_status`: "Delayed" or "On Time"

### Sample Queries

```sql
-- Count delayed vs on-time flights
SELECT delay_status, COUNT(*) as count
FROM flights
GROUP BY delay_status;

-- Average delay by airline
SELECT AIRLINE, AVG(ARR_DELAY) as avg_delay
FROM flights
GROUP BY AIRLINE
ORDER BY avg_delay DESC;

-- Top 10 routes with most delays
SELECT ORIGIN, DEST, COUNT(*) as delayed_count
FROM flights
WHERE delay_status = 'Delayed'
GROUP BY ORIGIN, DEST
ORDER BY delayed_count DESC
LIMIT 10;
```

### Output Locations

- Parquet files: `output/flights_parquet/`
- DuckDB database: `output/flights.duckdb`
- Cleaned data: `output/flights_cleaned/`

## Future Improvements

- **Unified Pipeline**: Modify scripts to pass data between phases instead of reading from raw files each time
- **Error Handling**: Add try-except blocks and logging
- **Configuration File**: Externalize paths and Spark settings
- **Unit Tests**: Add tests for each component
- **Data Validation**: Add schema validation and quality checks
- **Join Airports Data**: Enrich flight data with airport information
- **Incremental Processing**: Support processing only new data
- **Monitoring Dashboard**: Add Prefect UI integration

