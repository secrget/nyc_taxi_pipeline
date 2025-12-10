SELECT
    month,
    avg(trip_minute) as avg_trip_minute,
    count(*) as total_trip
FROM {{ ref("longest_travel_time")}}
WHERE trip_minute >=0
GROUP BY month
ORDER BY month