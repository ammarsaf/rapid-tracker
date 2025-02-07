from prefect import flow
from prefect_github import GitHubCredentials
from dotenv import load_dotenv
import os
from prefect.runner.storage import GitRepository


from dotenv import load_dotenv
from pathlib import Path
import os
from main import main_flow
from pathlib import Path
import sys

load_dotenv(Path(f"{os.getcwd()}/.env"), override=True, verbose=True)


# github_credentials_block = GitHubCredentials(token=os.getenv("GITHUB_TOKEN"))
# github_credentials_block.save(name="my-github-credentials-block")

# Source for the code to deploy (here, a GitHub repo)
entry_file = "main.py"
entry_func = "main_flow"

if __name__ == "__main__":

    # source = GitRepository(
    #     url=SOURCE_REPO,
    #     credentials=GitHubCredentials.load("my-github-credentials-block"),
    #     branch="dev",
    # )
    if len(sys.argv) == 1:
        source_path = Path("../rapid-tracker")

    elif sys.argv[1] == "deploy":
        source_path = "https://github.com/ammarsaf/rapid-tracker"

    print("INFO: path", source_path)
    flow.from_source(
        source=source_path,
        entrypoint=f"{entry_file}:{entry_func}",  # Specific flow to run
    ).deploy(
        name="rapid-deployment",
        work_pool_name="rapid-work-pool",
        cron="* * * * *",  # Run every minute
    )

    # main_flow.serve(name="rapid-deployment", tags=["dev"])
