# Import SparkSession from PySpark
# SparkSession is the main entry point for using Apache Spark
from pyspark.sql import SparkSession

# Create and configure Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Validation message to confirm Spark started correctly
print("Spark Started Successfully")

# Read cleaned JSON dataset into Spark DataFrame
df = spark.read.json("data/airports_clean.json")

# Validation message to confirm JSON file loaded successfully
print("JSON Loaded Successfully")

# Validate and display DataFrame structure
# Shows column names and data types
df.printSchema()

# Validate and display first 5 records from dataset
df.show(5, truncate=False)

# Stop Spark session and release resources
spark.stop()

# Validation message to confirm program completed successfully
print("Done")