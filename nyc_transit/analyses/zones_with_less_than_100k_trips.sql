-- Analysis to count the number of taxi trips per zone, selecting zones with fewer than 100,000 trips
-- Source: Code Adapted from Week 5 materials duckdb.org and ChatGPT

SELECT 
    dl.zone, -- Select the zone name from the location dimension table
    COUNT(*) AS number_of_trips -- Count the total number of trips
FROM 
    {{ ref('mart__fact_all_taxi_trips') }} ft -- Using the all taxi trips fact table 
JOIN 
    {{ ref('mart__dim_locations') }} dl -- Joining with the location dimension table
ON 
    ft.pulocationid = dl.locationid -- Joining on location ID
GROUP BY 
    dl.zone -- Group by to aggregate trip counts per zone
HAVING 
    COUNT(*) < 100000 -- Applying Having as a filter to focus on zones with fewer than 100,000 trips


