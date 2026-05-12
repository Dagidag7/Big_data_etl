from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import duckdb

# =========================
# CREATE SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("Load Data") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# =========================
# READ CSV FILE
# =========================

flights_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

# =========================
# TRANSFORM DATA
# =========================

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

# =========================
# SHOW TRANSFORMED DATA
# =========================

print("\n=== TRANSFORMED DATA ===")

final_df.show(10)

final_df.printSchema()

# =========================
# SAVE AS PARQUET
# =========================

parquet_path = "output/flights_parquet"

final_df.write \
    .mode("overwrite") \
    .parquet(parquet_path)

print("\nParquet file saved successfully!")

# =========================
# LOAD INTO DUCKDB
# =========================

duckdb_path = "output/flights.duckdb"

conn = duckdb.connect(duckdb_path)

conn.execute(f"""
CREATE OR REPLACE TABLE flights AS
SELECT *
FROM parquet_scan('{parquet_path}/*.parquet')
""")

# =========================
# VERIFY DATA IN DUCKDB
# =========================

print("\n=== TABLES IN DUCKDB ===")

print(conn.execute("SHOW TABLES").fetchall())

print("\n=== SAMPLE DATA FROM DUCKDB ===")

print(conn.execute("SELECT * FROM flights LIMIT 10").fetchdf())

print("\nData loaded into DuckDB successfully!")

# =========================
# CLOSE CONNECTIONS
# =========================

conn.close()

spark.stop()

print("\nETL PIPELINE COMPLETED SUCCESSFULLY!")