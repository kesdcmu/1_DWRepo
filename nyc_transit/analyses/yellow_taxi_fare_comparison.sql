-- Analysis of average taxi fare amounts by borough and zone
-- Source: Code Adapted from Week 5 materials duckdb.org and ChatGPT

SELECT 
    yt.*, -- Select everything from yellow taxi trip table
    dl.borough, -- Including borough information from location table
    dl.zone, -- Including zone information from location table
    AVG(yt.fare_amount) OVER (PARTITION BY dl.borough) AS avg_fare_borough, -- Window function: Average fare by borough
    AVG(yt.fare_amount) OVER (PARTITION BY dl.zone) AS avg_fare_zone, -- Window function: Average fare by zone
    AVG(yt.fare_amount) OVER () AS avg_fare_overall -- Window function: Overall average fare across all trips
FROM 
    {{ ref('stg__yellow_tripdata') }} yt -- Staged yellow taxi table
INNER JOIN 
    {{ ref('mart__dim_locations') }} dl -- Inner join with dimensional location table
ON 
    yt.PULocationID = dl.locationid; -- Join connection based on location ID

