{{
    config(materialized='view')
}}
SELECT 
    driver_name, 
    dates_warning, MAX(date) as date
FROM {{ ref('fact_history_cumulated') }}
GROUP BY driver_name , dates_warning;