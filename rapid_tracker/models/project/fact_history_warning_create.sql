CREATE TABLE IF NOT EXISTS rapidkl.warning_cumulated (
	driver_name TEXT,  
	dates_warning DATE[], 
	date DATE, 
	PRIMARY KEY (driver_name, date)
)