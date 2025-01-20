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
- fact_driving_behavior (aggregate daily) ✅
- fact_history_alarm (accumulated) 
- fact_bus_maintenance (aggregate daily) 🟨
    - code static (alter require for automation)

**Infra**
- Postgres - database server
- Airflow - automate table creation 
- Dbt - modularize sql script
- Apache Superset - visualize data

**Improvement**
- data integrity during table insertion (bug from Pandas `.to_sql()`)
