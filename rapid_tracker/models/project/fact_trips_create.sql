CREATE TABLE IF NOT EXISTS rapidkl.fact_trips (
    timestamp TIMESTAMP, 
    trip_id TEXT, 
    start_time TEXT, 
    driver_name TEXT,
    bus_plates TEXT, 
    route_id TEXT, 
    speed REAL,
    PRIMARY KEY (trip_id, timestamp, bus_plates, speed, route_id)
)