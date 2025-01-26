from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO = "https://github.com/ammarsaf/rapid-tracker.git"
entry_file = "main.py"
entry_func = "main_flow"

if __name__ == "__main__":

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint=f"{entry_file}:{entry_func}",  # Specific flow to run
    ).deploy(
        name="rapid-tracker-deployment",
        work_pool_name="tracker-work-pool",
        cron="0 * * * *",  # Run every hour
    )
