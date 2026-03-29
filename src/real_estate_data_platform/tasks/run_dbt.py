"""Prefect task for executing dbt commands."""

from dotenv import load_dotenv
from prefect import get_run_logger, task
from prefect.cache_policies import NONE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@task(cache_policy=NONE, retries=1, retry_delay_seconds=30)
def run_dbt(args: list[str], project_dir: str, profiles_dir: str) -> None:
    """Execute a dbt command via programmatic invocation.

    Uses ``PrefectDbtRunner`` (prefect-dbt ≥ 0.7) which invokes dbt
    in-process — no subprocess, no PATH issues.  Each dbt node is
    automatically reflected as a Prefect task for observability.

    Args:
        args: dbt CLI arguments (e.g., ``["snapshot"]``).
        project_dir: Path to the dbt project directory.
        profiles_dir: Path to the dbt profiles directory.
    """
    logger = get_run_logger()
    logger.info("Running dbt %s (project_dir=%s)", " ".join(args), project_dir)

    load_dotenv(override=False)

    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
        ),
    )
    runner.invoke(args)
