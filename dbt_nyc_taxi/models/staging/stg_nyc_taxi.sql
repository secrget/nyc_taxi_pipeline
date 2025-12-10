SELECT {{ select_all("public","fact_trips") }}
FROM {{ source("public","fact_trips")}}
