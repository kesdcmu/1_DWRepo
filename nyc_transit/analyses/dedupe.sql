-- Analysis selecting the most recent taxi trip for each pickup location using QUALIFY
-- Source: Adapted from Week 5 materials, duckdb.org, and ChatGPT

SELECT 
    * -- Select all columns from the taxi trips fact table
FROM 
    {{ ref('mart__fact_all_taxi_trips') }} -- Referencing the fact table for all taxi trips, ref vs hard code
QUALIFY 
    ROW_NUMBER() OVER (
        PARTITION BY PUlocationID  -- Partition data by pickup location ID
        ORDER BY pickup_datetime DESC -- Order trips within each partition by pickup datetime in descending order
    ) = 1; -- Use QUALIFY to filter for the most recent trip per pickup location


