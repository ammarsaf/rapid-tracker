SELECT 
    driver_name, 
    dates_warning, MAX(date) as date
FROM rapidkl.warning_cumulated
GROUP BY driver_name , dates_warning;