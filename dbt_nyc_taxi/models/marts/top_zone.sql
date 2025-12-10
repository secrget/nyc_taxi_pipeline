SELECT
    zn.zone,
    zn.location_id,
    count(*) as total_trips
FROM {{ ref("stg_nyc_taxi") }} ft
JOIN {{ source("public","dim_zones") }} zn
    ON ft.pulocationid = zn.location_id
GROUP BY zn.zone, zn.location_id
ORDER BY total_trips DESC
