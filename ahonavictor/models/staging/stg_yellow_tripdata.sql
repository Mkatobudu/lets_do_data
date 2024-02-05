{{config(materialized='view')}}

select 

--identifiers
md5(cast(coalesce(cast("yellow_trips_data"."VendorID" as TEXT), '_dbt_utils_surrogate_key') as TEXT)),
cast("VendorID" as integer) as VendorID,
cast("RatecodeID" as integer) as ratecodeid,
cast("PULocationID" as integer) as pickup_locationid,
cast("DOLocationID" as integer) as dropoff_locationid,

--timestamps
cast("tpep_pickup_datetime" as timestamp) as pickup_datetime,
cast("tpep_dropoff_datetime" as timestamp) as dropoff_datetime,

--tripinfo
store_and_fwd_flag,
cast("passenger_count" as integer) as passenger_count,
cast("trip_distance" as numeric) as trip_distance,

--payment info
cast("fare_amount" as numeric) as fare_amount,
cast("extra" as numeric) as extra,
cast("mta_tax" as numeric) as mta_tax,
cast("tip_amount" as numeric) as tip_amount,
cast("tolls_amount" as numeric) as tolls_amount,
cast("improvement_surcharge" as numeric) as improvement_surcharge,
cast("total_amount" as numeric) as total_amount,
cast("payment_type" as integer) as payment_type,
{{get_payment_type_description("payment_type")}} as payment_type_description,
cast("congestion_surcharge" as numeric) as congestion_surcharge

from {{source('staging', 'yellow_trips_data')}}
where "VendorID" is not null


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}