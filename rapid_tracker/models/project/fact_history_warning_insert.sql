WITH 
date_vars AS (
    SELECT 
        CURRENT_DATE AS today,
        DATE(CURRENT_DATE - INTERVAL '1 day') AS yesterday
), 
driver_behavior AS (
	SELECT * FROM (
	SELECT
	    DATE(timestamp) as date, 
	    bus_plates, 
	    driver_name, 
	    CASE 
	        WHEN speed > 60 THEN 1
	        ELSE 0
	    END count_breach_speed
	FROM rapidkl.fact_trips
	) hhh
	WHERE count_breach_speed > 0
),
yesterday AS (
	SELECT *
	FROM rapidkl.warning_cumulated
	WHERE date = DATE(SELECT yesterday FROM date_vars)
), 
today AS  (
	SELECT 
		*
	FROM driver_behavior
	WHERE date = DATE(SELECT today FROM date_vars)
	GROUP BY date, bus_plates, driver_name, count_breach_speed 
)
SELECT 
	COALESCE (t.driver_name, y.driver_name) as driver_name, 
	CASE 
		WHEN y.dates_warning IS NULL THEN ARRAY[t.date]
		WHEN t.date IS NULL THEN y.dates_warning
		ELSE ARRAY [t.date] || y.dates_warning
	END as date_warning,
	COALESCE (t.date, y.date + INTERVAL '1 day') as date
FROM today t
FULL OUTER JOIN yesterday y
ON t.driver_name = y.driver_name;