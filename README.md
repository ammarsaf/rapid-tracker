# rapid-tracker
etl with RapidKL GTFS API data monitor 

# TODO

**Functions**
- request_api_rapidkl() ✅
- query_db() ✅
- fetch_db() ✅

**Table**
- fact_daily_trip (accumulated) ✅
- fact_trips (accumulated) ✅
- dim_drivers (static) ✅
- dim_busses (static) ✅
- fact_driving_behavior (static daily) ✅
- fact_history_alarm (accumulated)

**Improvement**
- data integrity during table insertion (bug from Pandas `.to_sql()`)
