COPY ( -- code adopted from Week 4 lecture 4 speed run and chatgbt 
SELECT
    count(*) as num_airport_trips,
FROM {{ ref('mart__fact_all_taxi_trips') }}
WHERE dolocationid = 1
   OR dolocationid = 132
   OR dolocationid = 138
GROUP BY all
) TO 'C:\Users\KaiES\OneDrive\Documents\1_DWRepo\answers/2_HW4taxi_trips_ending_at_airport.txt' WITH CSV HEADER; --added output for HW review 
