-- Analysis calculating the average time between taxi pickups for each zone
-- Source: Adapted from Week 5 materials, duckdb.org, and ChatGPT 

WITH TimeDifferences AS (
    SELECT 
        pickup_borough AS zone, -- Treat pickup_borough as 'zone' for simplification of analysis
        started_at_ts, -- Timestamp when the taxi pickup started
        LEAD(started_at_ts) OVER (
            PARTITION BY pickup_borough ORDER BY started_at_ts
        ) AS next_pickup_time, -- Timestamp of the next pickup in the same zone using LEAD window function
        EXTRACT(EPOCH FROM (
            LEAD(started_at_ts) OVER (PARTITION BY pickup_borough ORDER BY started_at_ts) - started_at_ts
        )) AS time_diff_seconds -- Calculate the time difference in seconds to the next pickup
    FROM 
        {{ ref('mart__fact_trips_by_borough') }} -- Reference the trips by borough fact table
)

SELECT 
    zone, -- Zone name
    AVG(time_diff_seconds) AS avg_time_between_pickups_seconds -- Calculate the average time between pickups in seconds
FROM 
    TimeDifferences
WHERE 
    time_diff_seconds IS NOT NULL -- Ensuring valid time differences are included
GROUP BY 
    zone; -- Group the results by zone





