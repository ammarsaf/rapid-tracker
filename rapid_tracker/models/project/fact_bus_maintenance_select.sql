with geo_table AS (
    SELECT timestamp, 
            license_plate, 
            longitude, 
            latitude, 
            ROW_NUMBER () OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as row_number, 
            LAG (longitude) OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as prev_longitude, 
            LAG (latitude) OVER (PARTITION BY license_plate ORDER BY timestamp ASC) as prev_latitude
    FROM rapidkl.fact_daily_trip 
    ), 
    geo_today AS (
    SELECT 
            license_plate , 
            DATE(timestamp),
            SUM(distance_km) as total_distance_km
    FROM (
        SELECT timestamp, license_plate, 
            2 * 6371 * 
            ASIN(SQRT(
                POWER(SIN(RADIANS(latitude - prev_latitude) / 2), 2) +
                COS(RADIANS(prev_latitude)) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS(longitude - prev_longitude) / 2), 2)
            )) AS distance_km
        FROM geo_table
    ) jjj
    WHERE DATE(timestamp) = DATE('{str(today)}') -- today
    GROUP BY license_plate, distance_km, timestamp
    ORDER BY distance_km DESC
    ), 
    geo_yesterday AS (
    SELECT 
            license_plate , 
            DATE(timestamp),
            SUM(distance_km) as total_distance_km
    FROM (
        SELECT timestamp, license_plate, 
            2 * 6371 * 
            ASIN(SQRT(
                POWER(SIN(RADIANS(latitude - prev_latitude) / 2), 2) +
                COS(RADIANS(prev_latitude)) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS(longitude - prev_longitude) / 2), 2)
            )) AS distance_km
        FROM geo_table
    ) jjj
    WHERE DATE(timestamp) = DATE('{str(yesterday)}') -- yesterday
    GROUP BY license_plate, distance_km, timestamp
    ORDER BY distance_km DESC
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