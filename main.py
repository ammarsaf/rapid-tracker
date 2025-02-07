from pipeline import request_api_rapidkl, rename_col
from db_conn import connect_db, connect_db_v2
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.events import emit_event
from prefect.automations import Automation
from prefect.events.schemas.automations import EventTrigger
from prefect_dbt.cli.commands import DbtCoreOperation
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime

load_dotenv(Path(f"{os.getcwd()}/.env"), override=True, verbose=True)


@task(
    name="Rapidkl API fetch",
    description="Fetching GTFS API from RapidKL",
    retries=3,
    retry_delay_seconds=3,
    log_prints=True,
)
def task_1_fetch_api():
    df_fetch = rename_col(request_api_rapidkl("rapid-bus-kl", datetime.now()))
    logger = get_run_logger()
    logger.info(f"Dataframe shape 1 {df_fetch.shape}")
    logger.info(f"DEBUG {datetime.now()} WOOIIIIIII")
    try:
        engine = connect_db_v2()
    except Exception as e1:
        logger.error("E1: ", e1)
        logger.error("Database failed to connect")
    try:
        df_fetch.to_sql(
            "fact_daily_trip", con=engine, schema="dev", if_exists="append", index=False
        )
        logger.info(f"fact_daily_trip successfully append")
    except Exception as e2:
        logger.error("E2: ", e2)
        logger.error("Database insertion has failed")
    try:
        emit_event(
            event="rapidkl.data.inserted",
            resource={"prefect.resource.id": "rapidkl.raw.insertion"},
        )
        logger.info("Event successfully created")
    except Exception as e3:
        logger.error("E3: ", e3)
        logger.error("Event failed to create")


@task
def task_2_trigger_dbt():
    logger = get_run_logger()
    try:
        logger = get_run_logger()
        result = DbtCoreOperation(
            commands=["dbt debug", "dbt run"],
            project_dir="../rapid-tracker/rapid_tracker/",
            profiles_dir="../rapid-tracker/rapid_tracker/",
            overwrite_profiles=False,
        ).run()
        logger.info("DBT insertion completed")
        return result
    except Exception as e:
        logger.error(f"Dbt Task ERROR: {e}")


@flow(log_prints=True)
def main_flow():
    task_1_fetch_api()
    task_2_trigger_dbt()
