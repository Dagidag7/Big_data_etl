from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ETL Pipeline") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# READ CSV FILE
# ==========================================

print("\n=== READING CSV FILE ===")

flights_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

flights_df.show(5)
flights_df.printSchema()

# ==========================================
# READ JSON FILE
# ==========================================

print("\n=== READING JSON FILE ===")

airports_df = spark.read \
    .option("inferSchema", "true") \
    .json("data/airports_clean.json")

airports_df.show(5)
airports_df.printSchema()

# ==========================================
# BASIC CLEANING
# ==========================================

print("\n=== CLEANING DATA ===")

# Remove rows with missing important values
clean_flights_df = flights_df.dropna(
    subset=["FL_DATE", "AIRLINE", "ORIGIN", "DEST"]
)

# Fill delay nulls with 0
clean_flights_df = clean_flights_df.fillna({
    "DEP_DELAY": 0,
    "ARR_DELAY": 0
})

# Create delay status column
clean_flights_df = clean_flights_df.withColumn(
    "DELAY_STATUS",
    (col("ARR_DELAY") > 15).cast("integer")
)

# ==========================================
# SAVE CLEANED DATA
# ==========================================

print("\n=== SAVING CLEANED DATA ===")

clean_flights_df.write \
    .mode("overwrite") \
    .parquet("output/flights_cleaned")

airports_df.write \
    .mode("overwrite") \
    .parquet("output/airports_cleaned")

print("\n=== ETL EXTRACTION COMPLETED SUCCESSFULLY ===")

# ==========================================
# STOP SPARK
# ==========================================

spark.stop()