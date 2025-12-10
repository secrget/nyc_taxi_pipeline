from pyspark.sql import SparkSession
import os

# ----------------------------
# Створюємо SparkSession
# ----------------------------
spark = SparkSession.builder \
    .appName("RawToBronze") \
    .getOrCreate()

# ----------------------------
# Базова директорія проєкту
# ----------------------------
# Скрипт лежить у spark/jobs
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

# ----------------------------
# Папки для даних
# ----------------------------
raw_folder = os.path.join(project_root, "data/raw/taxi")
bronze_folder = os.path.join(project_root, "data/bronze/taxi")
os.makedirs(bronze_folder, exist_ok=True)

# ----------------------------
# Перелік місяців
# ----------------------------
months = [f"{i:02}" for i in range(1, 13)]  # 01–12

df_all = None

# ----------------------------
# Зчитування Parquet файлів і об'єднання
# ----------------------------
for month in months:
    file_name = f"yellow_tripdata_2024-{month}.parquet"
    raw_file_path = os.path.join(raw_folder, file_name)

    # Перевіряємо, чи файл існує
    if not os.path.exists(raw_file_path):
        print(f"Файл не знайдено: {raw_file_path}")
        continue
    print("Зчитування:",raw_file_path)
    # Читаємо Parquet
    df = spark.read.parquet(raw_file_path)

    # Об'єднуємо всі місяці
    if df_all is None:
        df_all = df
    else:
        df_all = df_all.unionByName(df)

# ----------------------------
# Запис у Bronze як один CSV
# ----------------------------
if df_all is not None:
    output_path = os.path.join(bronze_folder)
    df_all.coalesce(1).write.option("header", "true") \
                              .option("delimiter", ",") \
                              .mode("overwrite") \
                              .parquet(output_path)
    print(f"Дані успішно збережені у Bronze: {output_path}")
else:
    print("Немає даних для запису!")

# ----------------------------
# Завершення Spark
# ----------------------------
print("Завершили")
spark.stop()
