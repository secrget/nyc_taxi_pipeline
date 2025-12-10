SELECT
    extract(HOUR from tpep_pickup_datetime) as hour,
    count(*) as trips
FROM {{ ref("stg_nyc_taxi") }}
GROUP BY hour
order by trips DESC