"""Response/result models for operations."""

from datetime import date, datetime

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import FlowStatus, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing


class _BaseResult(BaseModel):
    """Base model for operation results."""

    model_config = {"use_enum_values": True}


class ScrapeMetadata(_BaseResult):
    """Metadata about a scrape-to-bronze run, stored as JSON alongside the Parquet file."""

    mode: ScraperMode
    days: int
    specific_date: date | None = None
    max_pages: int
    record_count: int
    saved_at: datetime


class StorageResult(_BaseResult):
    """Result of a storage operation (e.g., saving to MinIO)."""

    path: str | None = None
    count: int = 0


class ScrapingResult(_BaseResult):
    """Result of scraping a page."""

    page_number: int
    listings: list[RentalsListing] = Field(default_factory=list)
    failed_listings: int = 0


class ScrapeToBronzeResult(_BaseResult):
    """Result of scrape-to-bronze flow execution."""

    status: FlowStatus
    total_listings: int = 0
    failed_listings: int = 0
    storage: StorageResult | None = None
    error: str | None = None


class BronzeToSilverResult(_BaseResult):
    """Result of bronze-to-silver flow execution."""

    status: FlowStatus
    source: str | None = None
    city: str | None = None
    partition_date: str | None = None
    records_read: int = 0
    records_loaded: int = 0
    error: str | None = None
