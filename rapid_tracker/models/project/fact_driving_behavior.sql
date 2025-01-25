{{
    config(materialized='table')
}}

WITH speed_count AS (
    SELECT 
        DATE(timestamp) as date, 
        bus_plates, 
        driver_name, 
        CASE 
            WHEN speed > 60 THEN 1
            ELSE 0
        END count_breach_speed
    FROM {{ ref('fact_trips') }}
    WHERE DATE(timestamp) = CURRENT_DATE
)
SELECT 
    date, 
    bus_plates, 
    driver_name, 
    CASE
        WHEN sum_breach_daily = 0 THEN 'Safe'
        WHEN sum_breach_daily = 1 THEN 'Cautious'
        WHEN sum_breach_daily = 2 THEN 'Cautious'
        WHEN sum_breach_daily > 2 THEN 'Danger'
    END behavior
FROM (
    SELECT 
        date, 
        bus_plates, 
        driver_name, 
        SUM(count_breach_speed) sum_breach_daily
    FROM speed_count
    GROUP BY date, bus_plates, driver_name
    ) hhh