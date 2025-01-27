from pipeline import request_api_rapidkl
from db_conn import connect_db
from datetime import datetime
from prefect import flow, task

engine = connect_db()


@task(
    name="Rapidkl API fetch",
    description="Fetching GTFS API from RapidLKKL",
    retries=3,
    retry_delay_seconds=3,
)
def task_1_fetch_api():
    df_fetch = request_api_rapidkl("rapid-bus-kl", datetime.now())
    print(df_fetch.shape)
    # df_fetch.to_sql('fact_daily_trip',
    #             con=engine,
    #             schema='dev',
    #             if_exists='replace',
    #             index=False)


@flow(log_prints=True)
def main_flow():
    task_1_fetch_api()


if __name__ == "__main__":
    main_flow()
