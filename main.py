from pipeline import request_api_rapidkl
from db_conn import connect_db
from datetime import datetime
from prefect import flow, task

engine = connect_db()


@task
def task_1_fetch_api():
    df_fetch = request_api_rapidkl("rapid-bus-kl", datetime.now())
    print("TASK 1 STATUS:", df_fetch.shape, df_fetch.columns)
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
