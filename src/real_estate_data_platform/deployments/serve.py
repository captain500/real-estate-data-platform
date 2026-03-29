"""Serve all Prefect deployments for the Real Estate Data Platform."""

from prefect import serve

from real_estate_data_platform.flows.bronze_to_silver_flow import bronze_to_silver
from real_estate_data_platform.flows.scrape_to_bronze_flow import scrape_to_bronze
from real_estate_data_platform.flows.silver_to_gold_flow import silver_to_gold
from real_estate_data_platform.models.enums import City, DataSource, DateMode
from real_estate_data_platform.scrapers.scraper_type import ScraperType

# ── Individual deployments (ad-hoc, no schedule) ─────────────────────
scrape_deployment = scrape_to_bronze.to_deployment(
    name="ad-hoc-scrape",
    parameters={
        "scraper_type": ScraperType.KIJIJI,
        "city": City.TORONTO,
        "mode": DateMode.LAST_X_DAYS,
        "days": 1,
        "max_pages": 20,
    },
    tags=["scraping", "bronze"],
    description="Scrape rental listings and store raw Parquet in MinIO. Trigger manually.",
)

bronze_to_silver_deployment = bronze_to_silver.to_deployment(
    name="ad-hoc-bronze-to-silver",
    parameters={
        "source": DataSource.KIJIJI,
        "mode": DateMode.LAST_X_DAYS,
        "days": 1,
    },
    tags=["etl", "silver"],
    description="Read raw data from MinIO and upsert into PostgreSQL silver. Trigger manually.",
)

silver_to_gold_deployment = silver_to_gold.to_deployment(
    name="ad-hoc-silver-to-gold",
    tags=["dbt", "gold"],
    description="Run dbt snapshot + models to build gold layer. Trigger manually.",
)

if __name__ == "__main__":
    serve(
        scrape_deployment,
        bronze_to_silver_deployment,
        silver_to_gold_deployment,
    )
