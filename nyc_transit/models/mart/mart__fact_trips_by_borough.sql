-- combination of the dim_locations and mart fact table for all trips using a left join

with trips_renamed as (
    select 'bike' as type, started_at_ts, ended_at_ts from {{ ref('stg__bike_data') }}
    union all
    select 'fhv' as type, pickup_datetime, dropoff_datetime from {{ ref('stg__fhv_tripdata') }}
    union all
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime from {{ ref('stg__fhvhv_tripdata') }}
    union all
    select 'green' as type, lpep_pickup_datetime, lpep_dropoff_datetime from {{ ref('stg__green_tripdata') }}
    union all
    select 'yellow' as type, tpep_pickup_datetime, tpep_dropoff_datetime from {{ ref('stg__yellow_tripdata') }}
)

select
    type,
    started_at_ts,
    ended_at_ts,
    datediff('minute', started_at_ts, ended_at_ts) as duration_min,
    datediff('second', started_at_ts, ended_at_ts) as duration_sec,
    pickup_borough.borough as pickup_borough
from trips_renamed
left join {{ ref('taxi+_zone_lookup') }} as pickup_borough
    on trips_renamed.type = pickup_borough.borough
