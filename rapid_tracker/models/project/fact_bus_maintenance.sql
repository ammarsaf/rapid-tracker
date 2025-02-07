{{
    config(materialized='table')
}}

WITH date_vars AS (
    SELECT 
        CURRENT_DATE AS today,
        DATE(CURRENT_DATE - INTERVAL '1 day') AS yesterday
), 
geo_table AS (
    SELECT TO_TIMESTAMP(CAST(timestamp as INT))::date as timestamp, 
            license_plate, 
            longitude, 
            latitude, 
            ROW_NUMBER () OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as row_number, 
            LAG (longitude) OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as prev_longitude, 
            LAG (latitude) OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as prev_latitude
    FROM dev.fact_daily_trip --- TODO
), 
calculate_distance AS (
    SELECT timestamp, license_plate, 
            2 * 6371 * 
            ASIN(SQRT(
                POWER(SIN(RADIANS(latitude - prev_latitude) / 2), 2) +
                COS(RADIANS(prev_latitude)) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS(longitude - prev_longitude) / 2), 2)
            )) AS distance_km
    FROM geo_table
),
geo_today AS (
    SELECT 
            license_plate , 
            DATE(timestamp),
            SUM(distance_km) as total_distance_km
    FROM calculate_distance
    WHERE DATE(timestamp) = (SELECT today FROM date_vars) -- today
    GROUP BY license_plate, distance_km, timestamp
    ), 
geo_yesterday AS (
    SELECT 
            license_plate , 
            DATE(timestamp),
            SUM(distance_km) as total_distance_km
    FROM calculate_distance
    WHERE DATE(timestamp) = (SELECT yesterday FROM date_vars) -- yesterday
    GROUP BY license_plate, distance_km, timestamp
    ), 
total_distance_today AS (
    SELECT license_plate, SUM(total_distance_km) AS total_distance_km_today
    FROM (
        SELECT *
        FROM geo_today
        UNION ALL
        SELECT * 
        FROM geo_yesterday
    ) kkk
    GROUP BY license_plate
    )
SELECT CURRENT_DATE as date, *, 
CASE
    WHEN total_distance_km_today > 10000 THEN 'Need service'
    ELSE 'Good'
END AS bus_condition
FROM total_distance_today 