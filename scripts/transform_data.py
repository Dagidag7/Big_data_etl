from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark environment

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Transform Data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# =========================
# IMPORT SOURCE FILES
# =========================

flights_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

airports_df = spark.read \
    .option("multiline", "true") \
    .json("data/airports_clean.json")

# =========================
# FILTER INVALID RECORDS
# =========================

# Exclude rows with missing arrival delay data
flights_cleaned = flights_df.dropna(subset=["ARR_DELAY"])

# Keep flights that completed successfully
flights_cleaned = flights_cleaned.filter(col("CANCELLED") == 0)

# =========================
# ADD STATUS LABEL
# =========================

flights_cleaned = flights_cleaned.withColumn(
    "delay_status",
    when(col("ARR_DELAY") > 15, "Delayed")
    .otherwise("On Time")
)

# =========================
# RETRIEVE REQUIRED FIELDS
# =========================

final_df = flights_cleaned.select(
    "FL_DATE",
    "AIRLINE",
    "ORIGIN",
    "DEST",
    "ARR_DELAY",
    "delay_status"
)

# =========================
# OUTPUT TRANSFORMED DATA
# =========================

print("\n=== TRANSFORMED DATA ===")
final_df.show(10)

final_df.printSchema()