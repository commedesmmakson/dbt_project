WITH hourly_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TRIM(TO_CHAR(timestamp, 'Day')) AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('Month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
    WHEN time >= TIME '21:00' OR time < TIME '05:00' THEN 'night'
    WHEN time >= TIME '05:00' AND time < TIME '17:00' THEN 'day'
    WHEN time >= TIME '17:00' AND time < TIME '21:00' THEN 'evening'
END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features