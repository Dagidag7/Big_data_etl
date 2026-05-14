from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import duckdb

# =========================================================
# INITIALIZE APACHE SPARK SESSION FOR ETL PROCESSING
# This session acts as the main entry point for Spark
# operations such as reading, transforming, and writing data.
# =========================================================

spark = SparkSession.builder \
    .appName("Load Data") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# =========================================================
# LOAD FLIGHT DATASET FROM CSV FILE
# - Reads structured flight information
# - Uses first row as column headers
# - Automatically detects column data types
# =========================================================

flights_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

# =========================================================
# DATA TRANSFORMATION PHASE
# Selected Important Columns:
#   - Flight Date
#   - Airline
#   - Origin Airport
#   - Destination Airport
#   - Arrival Delay
#
# Business Logic:
# Create a new column called 'delay_status'
#   - If arrival delay > 15 minutes  → "Delayed"
#   - Otherwise                     → "On Time"
# =========================================================

final_df = flights_df.select(
    "FL_DATE",
    "AIRLINE",
    "ORIGIN",
    "DEST",
    "ARR_DELAY"
).withColumn(
    "delay_status",
    when(col("ARR_DELAY") > 15, "Delayed")
    .otherwise("On Time")
)

# =========================================================
# DISPLAY TRANSFORMED DATA FOR VALIDATION
# - Shows first 10 processed records
# - Prints dataframe schema and data types
# =========================================================

print("\n=== TRANSFORMED DATA ===")

final_df.show(10)

final_df.printSchema()

# =========================================================
# STORE TRANSFORMED DATA IN PARQUET FORMAT
# Parquet Advantages:
#   - Columnar storage format
#   - Faster querying performance
#   - Efficient compression
#   - Commonly used in big data systems
# =========================================================

parquet_path = "output/flights_parquet"

final_df.write \
    .mode("overwrite") \
    .parquet(parquet_path)

print("\nParquet file saved successfully!")

# =========================================================
# CONNECT TO DUCKDB DATABASE
# DuckDB is used as an analytical database engine
# for querying parquet-based datasets efficiently.
# =========================================================

duckdb_path = "output/flights.duckdb"

conn = duckdb.connect(duckdb_path)

conn.execute(f"""
CREATE OR REPLACE TABLE flights AS
SELECT *
FROM parquet_scan('{parquet_path}/*.parquet')
""")

# =========================================================
# VERIFY SUCCESSFUL DATA LOADING INTO DUCKDB
# - Display available database tables
# - Retrieve sample records for confirmation
# =========================================================

print("\n=== TABLES IN DUCKDB ===")

print(conn.execute("SHOW TABLES").fetchall())

print("\n=== SAMPLE DATA FROM DUCKDB ===")

print(conn.execute("SELECT * FROM flights LIMIT 10").fetchdf())

print("\nData loaded into DuckDB successfully!")

# =========================================================
# RELEASE SYSTEM RESOURCES
# - Close DuckDB database connection
# - Stop Spark session cleanly
# =========================================================

conn.close()

spark.stop()

# =========================================================
# FINAL SUCCESS MESSAGE
# Indicates that the complete ETL pipeline
# executed successfully from extraction
# to transformation and loading.
# =========================================================

print("\nETL PIPELINE COMPLETED SUCCESSFULLY!")