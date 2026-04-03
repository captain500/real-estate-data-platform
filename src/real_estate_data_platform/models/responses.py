"""Response/result models for operations."""

from datetime import date, datetime
from typing import Self

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import DateMode, FlowStatus
from real_estate_data_platform.models.listings import RentalsListing


class _BaseResult(BaseModel):
    """Base model for operation results."""

    model_config = {"use_enum_values": True}


class ScrapeMetadata(_BaseResult):
    """Metadata about a scrape-to-bronze run, stored as JSON alongside the Parquet file."""

    mode: DateMode
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


class PartitionResult(_BaseResult):
    """Result of processing a single (source, city, date) partition."""

    status: FlowStatus
    source: str
    city: str
    partition_date: str
    rows_read: int = 0
    rows_loaded: int = 0
    neighbourhoods_loaded: int = 0
    error: str | None = None


class BronzeToSilverResult(_BaseResult):
    """Result of the full bronze-to-silver flow execution."""

    status: FlowStatus
    total_read: int = 0
    total_loaded: int = 0
    total_neighbourhoods_loaded: int = 0
    partitions_ok: int = 0
    partitions_error: int = 0
    partitions_no_data: int = 0
    partition_results: list[PartitionResult] = Field(default_factory=list)
    error: str | None = None

    @classmethod
    def from_partitions(cls, results: list[PartitionResult]) -> Self:
        """Aggregate individual partition results into a single flow result.

        Determines the overall status:
        - ERROR if any partition errored.
        - COMPLETED_NO_DATA if no rows were read at all.
        - SUCCESS otherwise.
        """
        total_read = sum(r.rows_read for r in results)
        total_loaded = sum(r.rows_loaded for r in results)
        total_neighbourhoods_loaded = sum(r.neighbourhoods_loaded for r in results)
        ok = sum(1 for r in results if r.status == FlowStatus.SUCCESS)
        errors = sum(1 for r in results if r.status == FlowStatus.ERROR)
        no_data = sum(1 for r in results if r.status == FlowStatus.COMPLETED_NO_DATA)

        if errors:
            status = FlowStatus.ERROR
        elif total_read == 0:
            status = FlowStatus.COMPLETED_NO_DATA
        else:
            status = FlowStatus.SUCCESS

        return cls(
            status=status,
            total_read=total_read,
            total_loaded=total_loaded,
            total_neighbourhoods_loaded=total_neighbourhoods_loaded,
            partitions_ok=ok,
            partitions_error=errors,
            partitions_no_data=no_data,
            partition_results=results,
        )


class SilverToGoldResult(_BaseResult):
    """Result of the silver-to-gold flow execution."""

    status: FlowStatus
    error: str | None = None
