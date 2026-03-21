"""Prefect flow for moving data from Silver to Gold layer using dbt."""

from prefect import flow, get_run_logger

from real_estate_data_platform.config.settings import settings
from real_estate_data_platform.models.enums import FlowStatus
from real_estate_data_platform.models.responses import SilverToGoldResult
from real_estate_data_platform.tasks.run_dbt import run_dbt


@flow(name="silver-to-gold")
def silver_to_gold() -> SilverToGoldResult:
    """Main flow to apply SCD2 from silver to gold using dbt.

    Executes three dbt steps sequentially:

    1. ``dbt snapshot`` — applies SCD2 logic on ``silver.rentals_listings``,
       creating / updating versioned rows in ``gold._snap_fact_rentals_listings``.
    2. ``dbt run --select fact_rentals_listings`` — refreshes the view that
       exposes clean temporal columns (``valid_from``, ``valid_to``, ``is_current``).
    3. ``dbt run --select dim_neighbourhood`` — refreshes the neighbourhood
       dimension (insert-only, no SCD2).

    Returns:
        SilverToGoldResult with execution status.
    """
    logger = get_run_logger()
    dbt_cfg = settings.dbt

    logger.info(
        "Starting silver to gold flow (project_dir=%s, profiles_dir=%s)",
        dbt_cfg.project_dir,
        dbt_cfg.profiles_dir,
    )

    # 1. Apply SCD2 via using snapshot
    try:
        run_dbt(
            args=["snapshot"],
            project_dir=dbt_cfg.project_dir,
            profiles_dir=dbt_cfg.profiles_dir,
        )
    except Exception:
        logger.error("dbt snapshot failed", exc_info=True)
        return SilverToGoldResult(status=FlowStatus.ERROR, error="dbt snapshot failed")

    # 2. Refresh gold models (fact view + dim table)
    try:
        run_dbt(
            args=["run", "--select", "fact_rentals_listings", "dim_neighbourhood"],
            project_dir=dbt_cfg.project_dir,
            profiles_dir=dbt_cfg.profiles_dir,
        )
    except Exception:
        logger.error("dbt run failed", exc_info=True)
        return SilverToGoldResult(status=FlowStatus.ERROR, error="dbt run failed")

    logger.info("Silver to Gold flow completed successfully")
    return SilverToGoldResult(status=FlowStatus.SUCCESS)


if __name__ == "__main__":
    result = silver_to_gold()
