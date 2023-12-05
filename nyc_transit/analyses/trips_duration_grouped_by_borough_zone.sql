-- Analysis of the number of trips and average duration by borough and zone
-- Source: Code Adapted from Week 5 materials, duckdb.org, and ChatGPT 

SELECT  
    dl.borough, -- Select borough information from the location dimension table
    dl.zone, -- Select zone information from the location dimension table
    COUNT(*) AS number_of_trips, -- Count the total number of taxi trips per borough and zone
    AVG(tt.duration_min) AS average_duration_minutes -- Average duration in minutes for trips in each borough and zone
FROM 
    {{ ref('mart__fact_all_taxi_trips') }} tt -- Using the fact table for all taxi trips, ref vs hard code 
JOIN 
    {{ ref('mart__dim_locations') }} dl -- Joining with the dimension location table, ref vs hard code 
ON 
    tt.pulocationid = dl.locationid -- Joining on location ID to connect trips with specific boroughs and zones
GROUP BY 
    dl.borough, dl.zone; -- Group by trip results on both borough and zone



