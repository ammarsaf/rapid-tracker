# rapid-tracker
etl with RapidKL GTFS API data monitor 

# TODO

**Functions**
- request_api_rapidkl() ✅
- query_db() ✅
- fetch_db() ✅

**Table**
- fact_daily_trip (append) ✅
- fact_trips (append) ✅
- dim_drivers (static) ✅
- dim_busses (static) ✅
- fact_driving_behavior (aggregate daily) ✅
- fact_history_alarm (accumulated) 
- fact_bus_maintenance (aggregate daily) ✅

**Containers**
- Postgres - database server ✅
- Airflow - automate table creation 
- Dbt - modularize sql script
- Apache Superset - visualize data

**Improvement**
- Data integrity during table insertion (bug from Pandas `.to_sql()`)
- Just use native SQL without using `df.to_sql()` (its possible)
- reduce GROUP BY and imply more window funcition OVER