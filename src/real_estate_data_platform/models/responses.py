"""Response/result models for operations."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import City, FlowStatus, OperationStatus, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing


class StorageResult(BaseModel):
    """Result of a storage operation (e.g., saving to MinIO)."""

    status: OperationStatus = Field(..., description="Operation status")
    path: str | None = Field(None, description="S3 object path")
    metadata_path: str | None = Field(None, description="S3 metadata object path")
    metadata: dict[str, Any] | None = Field(None, description="Metadata saved alongside the object")
    count: int = Field(default=0, description="Number of items stored")
    reason: str | None = Field(None, description="Reason for skip/failure")

    model_config = {"use_enum_values": True}


class ScrapingResult(BaseModel):
    """Result of scraping a page."""

    page_number: int = Field(..., description="Page number scraped")
    city: str = Field(..., description="City scraped")
    listings: list[RentalsListing] = Field(default_factory=list, description="Listings found")
    total_listings: int = Field(..., description="Total listings on page")
    error: str | None = Field(None, description="Error message if scraping failed")

    model_config = {"use_enum_values": True}


class ScrapeToBronzeResult(BaseModel):
    """Result of scrape-to-bronze flow execution."""

    status: FlowStatus = Field(..., description="Flow status: success, error, or completed_no_data")
    scraper_type: str = Field(..., description="Type of scraper used")
    city: City = Field(..., description="City scraped")
    mode: ScraperMode = Field(..., description="Scraper mode used (last_x_days or specific_date)")
    pages_scraped: int = Field(default=0, description="Number of pages attempted")
    successful_pages: int = Field(default=0, description="Number of pages successfully scraped")
    failed_pages: int = Field(default=0, description="Number of pages that failed")
    total_listings: int = Field(default=0, description="Total listings found")
    scrape_date: datetime = Field(..., description="Date and time of scrape")
    specific_date: date | None = Field(None, description="Specific date if mode is specific_date")
    days: int | None = Field(None, description="Number of days if mode is last_x_days")
    duration: float | None = Field(None, description="Duration of flow execution in seconds")
    storage: StorageResult | None = Field(None, description="Storage operation result")
    error: str | None = Field(None, description="Error message if flow failed")

    model_config = {"use_enum_values": True}
