-- Analysis calculating 7-day moving min, max, avg, and sum for precipitation and snow for each day
-- Source: Adapted from Week 5 materials, duckdb.org, and ChatGPT

WITH CentralWeatherData AS (
    SELECT 
        date, 
        prcp, 
        snow,
        ROW_NUMBER() OVER w AS rn, -- Assign row numbers for each record within the 7-day window
        MIN(prcp) OVER w AS min_prcp, -- Calculate the minimum precipitation
        MAX(prcp) OVER w AS max_prcp, -- Calculate the maximum precipitation
        AVG(prcp) OVER w AS avg_prcp, -- Calculate the average precipitation
        SUM(prcp) OVER w AS sum_prcp, -- Calculate the total precipitation
        MIN(snow) OVER w AS min_snow, -- Calculate the minimum snow
        MAX(snow) OVER w AS max_snow, -- Calculate the maximum snow
        AVG(snow) OVER w AS avg_snow, -- Calculate the average snow
        SUM(snow) OVER w AS sum_snow  -- Calculate the total snow
    FROM 
        {{ ref('stg__central_park_weather') }} -- Ref to central weather table vs hard code
    WINDOW w AS (
        ORDER BY date 
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING -- Define a 7-day window for each record
    )
)
SELECT 
    date, 
    min_prcp, 
    max_prcp, 
    avg_prcp, 
    sum_prcp,
    min_snow, 
    max_snow, 
    avg_snow, 
    sum_snow
FROM 
    CentralWeatherData;


