from prefect import flow
from prefect_github import GitHubCredentials
from dotenv import load_dotenv
import os
from prefect.runner.storage import GitRepository


load_dotenv()


github_credentials_block = GitHubCredentials(token=os.getenv("GITHUB_TOKEN"))
# github_credentials_block.save(name="my-github-credentials-block")

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO = "https://github.com/ammarsaf/rapid-tracker"
entry_file = "main.py"
entry_func = "main_flow"

if __name__ == "__main__":

    source = GitRepository(
        url=SOURCE_REPO,
        credentials=GitHubCredentials.load("my-github-credentials-block"),
        branch="dev",
    )

    flow.from_source(
        source=source,
        entrypoint=f"{entry_file}:{entry_func}",  # Specific flow to run
    ).deploy(
        name="rapid-deployment",
        work_pool_name="rapid-work-pool",
        cron="0 * * * *",  # Run every hour
    )
