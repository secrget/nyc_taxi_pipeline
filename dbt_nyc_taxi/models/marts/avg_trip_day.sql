SELECT
    date_trunc('day', tpep_pickup_datetime) as day,
    ROUND(avg(total_amount):: numeric, 3) as avg_total_amount,
    ROUND(avg(trip_distance):: numeric, 3) as avg_disance,
    count(*) as count
FROM {{ ref("stg_nyc_taxi") }}
GROUP BY 1
ORDER BY 1
