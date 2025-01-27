from pipeline import request_api_rapidkl
from db_conn import connect_db
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.events import emit_event
from prefect.automations import Automation
from prefect.events.schemas.automations import EventTrigger
from prefect_dbt.cli.commands import DbtCoreOperation

engine = connect_db()


@task(
    name="Rapidkl API fetch",
    description="Fetching GTFS API from RapidKL",
    retries=3,
    retry_delay_seconds=3,
    log_prints=True,
)
def task_1_fetch_api():
    df_fetch = request_api_rapidkl("rapid-bus-kl", datetime.now())
    logger = get_run_logger()
    logger.info(f"Dataframe shape 1 {df_fetch.shape}")
    df_fetch.to_sql(
        "fact_daily_trip", con=engine, schema="dev", if_exists="append", index=False
    )
    logger.info(f"fact_daily_trip successfully append")
    emit_event(
        event="rapidkl.data.inserted",
        resource={"prefect.resorce.id": "rapidkl.resource"},
    )


@task
def task_2_trigger_dbt_flow():
    logger = get_run_logger()
    try:
        logger = get_run_logger()
        result = DbtCoreOperation(
            commands=["dbt build -t dev"],
            project_dir="rapid-tracker",
            profiles_dir="/rapid-tracker/rapid_tracker",
        ).run()
        logger.info("INFO: DBT completed")
        return result
    except Exception as e:
        logger.error(f"ERROR: {e}")


@flow(log_prints=True)
def main_flow():
    task_1_fetch_api()
    task_2_trigger_dbt_flow()
