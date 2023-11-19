COPY ( -- code adopted from Week 4 lecture 4 speed run and chatgbt 
SELECT
    SELECT
        weekday(pickup_datetime) as weekday,
        pulocationid as start_location,
        dolocationid as end_location,
        COUNT(*) as total_trips,
        COUNT(CASE WHEN pulocationid <> dolocationid THEN 1 END) as inter_borough_trips,
        (COUNT(CASE WHEN pulocationid <> dolocationid THEN 1 END) * 100.0 / COUNT(*)) as inter_borough_percentage
    FROM {{ ref('mart__fact_all_taxi_trips') }}
    GROUP BY weekday, pulocationid, dolocationid
) TO 'C:\Users\KaiES\OneDrive\Documents\1_DWRepo\answers/3_HW4inter_borough_taxi_trips_by_weekday.txt' WITH CSV HEADER; --added output for HW review 