{{
    config(materialized='table') 
}}
SELECT
    NULL::TEXT AS driver_name,
    ARRAY[]::DATE[] AS dates_warning,
    NULL::DATE AS date
WHERE FALSE
/*
CREATE TABLE IF NOT EXISTS rapidkl.warning_cumulated (
	driver_name TEXT,  
	dates_warning DATE[], 
	date DATE, 
	PRIMARY KEY (driver_name, date)
)
*/