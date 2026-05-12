from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("Spark Started Successfully")

# Read cleaned JSON
df = spark.read.json("data/airports_clean.json")

print("JSON Loaded Successfully")

df.printSchema()

df.show(5, truncate=False)

spark.stop()

print("Done")