"""Response/result models for operations."""

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import FlowStatus, OperationStatus
from real_estate_data_platform.models.listings import RentalsListing


class _BaseResult(BaseModel):
    """Base model for operation results."""

    model_config = {"use_enum_values": True}


class StorageResult(_BaseResult):
    """Result of a storage operation (e.g., saving to MinIO)."""

    status: OperationStatus
    path: str | None = None
    count: int = 0
    error: str | None = None


class ScrapingResult(_BaseResult):
    """Result of scraping a page."""

    page_number: int
    listings: list[RentalsListing] = Field(default_factory=list)
    error: str | None = None


class ScrapeToBronzeResult(_BaseResult):
    """Result of scrape-to-bronze flow execution."""

    status: FlowStatus
    successful_pages: int = 0
    failed_pages: int = 0
    storage: StorageResult | None = None
    error: str | None = None
