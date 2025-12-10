import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


zone_path = "data/raw/taxi/taxi_zone_lookup.csv"

# Абсолютний шлях
os_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
zone_file = os.path.join(os_root, zone_path)

# Читаємо CSV через Pandas з явним кодуванням
try:
    zone_pd = pd.read_csv(zone_file, encoding='utf-8')  # якщо не пройде, спробуй 'latin1' або 'cp1252'
except UnicodeDecodeError:
    zone_pd = pd.read_csv(zone_file, encoding='latin1')

# Перейменування колонок
zone_pd = zone_pd.rename(columns={
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone"
})

# Переконатися, що всі рядки — тип str
for col_name in zone_pd.select_dtypes(['object']).columns:
    zone_pd[col_name] = zone_pd[col_name].astype(str)

# Підключення до PostgreSQL
try:
    conn = psycopg2.connect(
        host='172.27.208.1',
        port=5432,
        database='nyx-',
        user='postgres',
        password='postgres',
        options='-c client_encoding=UTF8'
    )
    cur = conn.cursor()
    print("Підключення до PostgreSQL успішне")
except Exception as e:
    print("Не вдалося підключитись до PostgreSQL:", e)
    exit(1)

# Створюємо таблицю, якщо її ще немає
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

# Вставка даних
insert_zone_query = """
INSERT INTO dim_zones (location_id, borough, zone, service_zone) 
VALUES %s
ON CONFLICT (location_id) DO NOTHING
"""
execute_values(cur, insert_zone_query, [tuple(x) for x in zone_pd.to_numpy()])

conn.commit()
cur.close()
conn.close()
print("Дані успішно завантажені у dim_zones")
