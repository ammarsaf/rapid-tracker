{{
    config(materialized='table') 
}}
SELECT 
    CAST(timestamp AS TIMESTAMP), 
    trip_id, 
    start_time, 
    driver_name, 
    bus_plates,
    route_id, 
    speed
FROM (
    SELECT *
    FROM dev.fact_daily_trip fdt
    JOIN dev.dim_busses vv 
    ON fdt.vehicle_id = vv.bus_plates
    JOIN dev.dim_drivers dd
    ON vv.bus_id = dd.driver_id
) jjj