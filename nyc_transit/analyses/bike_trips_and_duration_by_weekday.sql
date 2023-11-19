COPY ( -- code adopted from Week 4 lecture 4 speed run and chatgbt 
SELECT
    weekday(started_at_ts) as weekday,
    count(*) as total_trips,
    sum(duration_sec) as total_trip_duration_secs
from {{ ref('mart__fact_all_bike_trips') }}
group by all 
) TO 'C:\Users\KaiES\OneDrive\Documents\1_DWRepo\answers/1_HW4_bike_trips_and_duration_by_weekday.txt' WITH CSV HEADER; --added output for HW review 