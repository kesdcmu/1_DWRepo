-- Analysis to find taxi trips that lack a corresponding pickup location (Anti-Join)
-- Source: Adapted from Week 5 materials, duckdb.org, and ChatGPT

SELECT 
    * -- Select everything from the taxi trips fact table
FROM 
    {{ ref('mart__fact_all_taxi_trips') }} tt -- Fact table for all taxi trips, ref vs hard code 
WHERE 
    NOT EXISTS ( -- Apply an anti-join to select trips without a matching location
        SELECT 
            1
        FROM 
            {{ ref('mart__dim_locations') }} dl -- Referencing the dimension table for locations
        WHERE 
            tt.pulocationid = dl.locationid -- Condition for matching pickup location in the locations table
    );
