"""Response/result models for operations."""

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import FlowStatus, OperationStatus
from real_estate_data_platform.models.listings import RentalsListing


class StorageResult(BaseModel):
    """Result of a storage operation (e.g., saving to MinIO)."""

    status: OperationStatus = Field(..., description="Operation status")
    path: str | None = Field(None, description="S3 object path")
    count: int = Field(default=0, description="Number of items stored")
    error: str | None = Field(None, description="Error message if storage failed")

    model_config = {"use_enum_values": True}


class ScrapingResult(BaseModel):
    """Result of scraping a page."""

    page_number: int = Field(..., description="Page number scraped")
    listings: list[RentalsListing] = Field(default_factory=list, description="Listings found")
    error: str | None = Field(None, description="Error message if scraping failed")

    model_config = {"use_enum_values": True}


class ScrapeToBronzeResult(BaseModel):
    """Result of scrape-to-bronze flow execution."""

    status: FlowStatus = Field(..., description="Flow status: success, error, or completed_no_data")
    successful_pages: int = Field(default=0, description="Number of pages successfully scraped")
    failed_pages: int = Field(default=0, description="Number of pages that failed")
    storage: StorageResult | None = Field(None, description="Storage operation result")
    error: str | None = Field(None, description="Error message if flow failed")

    model_config = {"use_enum_values": True}
