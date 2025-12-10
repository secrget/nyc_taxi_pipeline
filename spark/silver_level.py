from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
import os

spark= SparkSession.builder\
    .appName("BronzeToSilver") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
bronze_folder = os.path.join(project_root, "data/bronze/taxi")
silver_folder= os.path.join(project_root,"data/silver/taxi")
df=spark.read.parquet(bronze_folder,sep=',', header=True, inferSchema=True)

df = df.dropDuplicates()

df = df.filter(col("passenger_count") >= 0)
df = df.filter(col("trip_distance") > 0)
df = df.withColumn("is_same_zone_trip", when(col("PULocationID") == col("DOLocationID"),1).otherwise(0))
# фільтр валідних дат
df = df.withColumn("pickup_datetime",
                   to_timestamp("tpep_pickup_datetime")) \
       .withColumn("dropoff_datetime",
                   to_timestamp("tpep_dropoff_datetime")) \
       .filter(col("pickup_datetime").isNotNull()) \
       .filter(col("dropoff_datetime").isNotNull()) \
       .filter(col("dropoff_datetime") > col("pickup_datetime"))

# кастинг чисел у правильні типи
numeric_cols = [
    "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "Airport_fee"
]

for c in numeric_cols:
    df = df.withColumn(c, col(c).cast("double"))

# ----------------------------
# 3️⃣ Розрахунок нових колонок
# ----------------------------

# Час поїздки в секундах
df = df.withColumn(
    "trip_time_sec",
    col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")
)

# Фільтр — відкидаємо поїздки > 4 год або < 1 хв
df = df.filter((col("trip_time_sec") > 60) & (col("trip_time_sec") < 14400))
df.coalesce(12).write.option("header", "true") \
                              .option("delimiter", ",") \
                              .mode("overwrite") \
                              .parquet(silver_folder)
print(f"Дані успішно збережені у silver: {silver_folder}")