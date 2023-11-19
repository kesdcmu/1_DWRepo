<<<<<<< HEAD
SELECT -- Week 4 lecture 4 Code Adoptation 
=======
SELECT
>>>>>>> 834546b24df8046fd3cfbbd0439eec96d396a7a5
    started_at_ts,
    ended_at_ts,
    datediff('minute', started_at_ts, ended_at_ts) as duration_min,
    datediff('second', started_at_ts, ended_at_ts) as duration_sec,
    start_station_id,
    end_station_id,
from {{ ref('stg__bike_data') }}
