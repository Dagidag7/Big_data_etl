from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Transform Data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# =========================
# READ DATASETS
# =========================

flights_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

airports_df = spark.read \
    .option("multiline", "true") \
    .json("data/airports_clean.json")

# =========================
# DATA CLEANING
# =========================

# Remove null values from ARR_DELAY
flights_cleaned = flights_df.dropna(subset=["ARR_DELAY"])

# Remove cancelled flights
flights_cleaned = flights_cleaned.filter(col("CANCELLED") == 0)

# =========================
# CREATE NEW COLUMN
# =========================

flights_cleaned = flights_cleaned.withColumn(
    "delay_status",
    when(col("ARR_DELAY") > 15, "Delayed")
    .otherwise("On Time")
)

# =========================
# SELECT IMPORTANT COLUMNS
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
# SHOW RESULT
# =========================

print("\n=== TRANSFORMED DATA ===")
final_df.show(10)

final_df.printSchema()