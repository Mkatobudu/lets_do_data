{{config(materialized='table')}}

SELECT
    cast("LocationID" as integer) as LocationID,
    cast("Borough" as text) as Borough,
    cast("Zone" as text) as Zone,
    replace(service_zone, 'Boro', 'Green') AS service_zone
FROM {{ref("taxi_zone_lookup")}}
