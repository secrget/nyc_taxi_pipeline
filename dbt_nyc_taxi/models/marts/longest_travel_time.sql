SELECT
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    extract(epoch from (tpep_dropoff_datetime  - tpep_pickup_datetime)) / 60 as trip_minute,
    extract(MONTH from tpep_pickup_datetime ) as month
FROM {{ ref("stg_nyc_taxi") }}
ORDER BY trip_minute DESC
