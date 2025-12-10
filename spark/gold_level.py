from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import psycopg2
import config
from dotenv import load_dotenv

spark = SparkSession.builder\
    .appName("SilverToGold")\
    .getOrCreate()
load_dotenv()
zone_path = "data/raw/taxi/taxi_zone_lookup.csv"
silver_data_path = "data/silver/taxi"


os_root=os.path.abspath(os.path.join(os.path.dirname(__file__),"../"))

silver_folder =os.path.join(os_root, silver_data_path)
zone_floder = os.path.join(os_root, zone_path)


gold_fact_trips_path = os.path.join(os_root, "data/gold/fact_trips")
gold_dim_zones_path = os.path.join(os_root, "data/gold/dim_zones")

# part-00000-e0c76217-5686-497e-bdaf-9fa1162c1924-c000.csv

trips = spark.read.csv(silver_folder, sep=',', header=True, inferSchema=True)
zone = spark.read.csv(zone_floder,sep=',', header=True, inferSchema=True)

trips = trips.join(
    zone.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("PUBorough"),
        col("Zone").alias("PUZone"),
        col("service_zone").alias("PUService_zone")
    ),
    on = "PULocationID",
    how = "inner"
)
trips = trips.join(
    zone.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("DOBorough"),
        col("Zone").alias("DOZone"),
        col("service_zone").alias("DOService_zone")
    ),
    on = "DOLocationID",
    how = "inner"
)
trips.show()
row_count = trips.count()
print("Count", row_count)

trips_pd = trips.toPandas()

try:
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD
    )
    cur = conn.cursor()
    print(" Підключення до PostgreSQL успішне")
except Exception as e:
    print(" Не вдалося підключитись до PostgreSQL:", e)
    exit(1)




create_table_query = """
CREATE TABLE IF NOT EXISTS fact_trips (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    PULocationID INT,
    PUBorough VARCHAR,
    PUZone VARCHAR,
    PUService_zone VARCHAR,
    DOLocationID INT,
    DOBorough VARCHAR,
    DOZone VARCHAR,
    DOService_zone VARCHAR,
    RatecodeID INT,
    store_and_fwd_flag VARCHAR,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT
)
"""
cur.execute(create_table_query)
conn.commit()

insert_query = """
INSERT INTO fact_trips VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for i, row in trips_pd.iterrows():
    cur.execute(insert_query, tuple(row))
conn.commit()


zone_pd = zone.toPandas()
zone_pd = zone_pd.rename(columns={
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone"
})

# ----------------------------
# Створюємо таблицю dim_zones, якщо її ще немає
# ----------------------------
create_zone_table_query = """
CREATE TABLE IF NOT EXISTS dim_zones (
    location_id INT PRIMARY KEY,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR
)
"""
cur.execute(create_zone_table_query)
conn.commit()

# ----------------------------
# Вставка даних у dim_zones
# ----------------------------
insert_zone_query = """
INSERT INTO dim_zones VALUES (%s, %s, %s, %s)
ON CONFLICT (location_id) DO NOTHING
"""
for i, row in zone_pd.iterrows():
    cur.execute(insert_zone_query, tuple(row))

conn.commit()
cur.close()
conn.close()
