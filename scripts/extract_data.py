from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================================================
# INITIALIZE SPARK SESSION
# Configure Spark application settings and memory allocation
# =========================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ETL Pipeline") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Reduce unnecessary Spark log messages
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# LOAD FLIGHT DATA FROM CSV FILE
# Read airline flight records with automatic schema detection
# =========================================================

print("\n=== READING CSV FILE ===")

flights_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/flights.csv")

# Display sample records and dataset structure
flights_df.show(5)
flights_df.printSchema()

# =========================================================
# LOAD AIRPORT DATA FROM JSON FILE
# Read airport reference dataset from JSON format
# =========================================================

print("\n=== READING JSON FILE ===")

airports_df = spark.read \
    .option("inferSchema", "true") \
    .json("data/airports_clean.json")

# Display sample records and dataset structure
airports_df.show(5)
airports_df.printSchema()

# =========================================================
# CLEAN AND TRANSFORM FLIGHT DATA
# Handle missing values and create derived columns
# =========================================================

print("\n=== CLEANING DATA ===")

# Remove rows missing critical flight information
clean_flights_df = flights_df.dropna(
    subset=["FL_DATE", "AIRLINE", "ORIGIN", "DEST"]
)

# Replace missing delay values with 0
clean_flights_df = clean_flights_df.fillna({
    "DEP_DELAY": 0,
    "ARR_DELAY": 0
})

# Create delay indicator column:
# 1 = delayed more than 15 minutes
# 0 = on-time or minor delay
clean_flights_df = clean_flights_df.withColumn(
    "DELAY_STATUS",
    (col("ARR_DELAY") > 15).cast("integer")
)

# =========================================================
# SAVE TRANSFORMED DATASETS
# Store cleaned datasets in Parquet format for analytics
# =========================================================

print("\n=== SAVING CLEANED DATA ===")

clean_flights_df.write \
    .mode("overwrite") \
    .parquet("output/flights_cleaned")

airports_df.write \
    .mode("overwrite") \
    .parquet("output/airports_cleaned")

print("\n=== ETL EXTRACTION COMPLETED SUCCESSFULLY ===")

# =========================================================
# TERMINATE SPARK SESSION
# Release Spark resources after pipeline execution
# =========================================================

spark.stop()
