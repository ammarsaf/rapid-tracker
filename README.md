# rapid-tracker
ELT with RapidKL GTFS real-time API data monitor 

# Get Started

1. Create virtualenv and install packages
```
python3 -m venv venvRapid
source venvRapid/bin/activate
pip install -r requirements.txt
```

2. Setup container 

`docker compose up`

3. Dbt path setup

```
export DBT_PROJECT_DIR=~/.dbt
export DBT_PROFILES_DIR/<project>/<working>/<directory>
```

3. Monitor visualization

- Go to ---- localhost:xxx


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
- fact_history_alarm (aggregate daily) ✅
- fact_bus_maintenance (aggregate daily) ✅

**Automate**
- fact_daily_trip (30s) [order 1]
- fact_trips (30s) [order 2]
- dim_drivers 
- dim_busses 
- fact_driving_behavior (1 day) [order 3]
- fact_history_alarm (1 day) [order 3]
- fact_bus_maintenance (1 day) [order 3]

**Infra**
- Docker container 
    - Postgres - database server ✅
    - Apache Airflow - automate table creation 
    - Apache Superset - visualize data
- Dbt 
    - modularize sql script ✅
    - Dbt schema 


**Improvement**
- Data integrity during table insertion (bug from Pandas `.to_sql()`)
- Just use native SQL without using `df.to_sql()` (its possible)
- reduce GROUP BY and imply more window funcition OVER