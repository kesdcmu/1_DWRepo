-- Analysis calculating the 7-day moving average of precipitation for each day
-- Source: Adapted from Week 5 materials, duckdb.org, and ChatGPT

SELECT 
    date, -- Select date from the central park weather data
    AVG(prcp) OVER (
        ORDER BY date 
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING -- Define a 7-day window 
    ) AS moving_avg_precipitation -- Calculate the moving average of precipitation over the 7-day window
FROM 
    {{ ref('stg__central_park_weather') }}; -- Reference the central park weather data table, ref vs hard code

