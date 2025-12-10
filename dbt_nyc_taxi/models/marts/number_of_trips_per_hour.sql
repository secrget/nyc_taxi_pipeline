SELECT
    date_trunc('hour', pickup_datetime) as hour,
    count(*) as trips_count
FROM {{ ref("longest_travel_time") }}
GROUP BY hour
ORDER BY 1
