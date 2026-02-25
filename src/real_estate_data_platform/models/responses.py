"""Response/result models for operations."""

from datetime import datetime

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import City, FlowStatus, OperationStatus


class StorageResult(BaseModel):
    """Result of a storage operation (e.g., saving to MinIO)."""

    status: OperationStatus = Field(..., description="Operation status")
    path: str | None = Field(None, description="S3 object path")
    count: int = Field(default=0, description="Number of items stored")
    reason: str | None = Field(None, description="Reason for skip/failure")
    timestamp: str = Field(..., description="Date string (YYYY-MM-DD)")

    model_config = {"use_enum_values": True}


class ScrapingResult(BaseModel):
    """Result of scraping a page."""

    page_number: int = Field(..., description="Page number scraped")
    city: str = Field(..., description="City scraped")
    listings: list = Field(default_factory=list, description="Listings found")
    total_listings: int = Field(..., description="Total listings on page")
    error: str | None = Field(None, description="Error message if scraping failed")


class FlowResult(BaseModel):
    """Result of a scraping flow execution."""

    status: FlowStatus = Field(..., description="Flow status: success, error, or completed_no_data")
    scraper_type: str = Field(..., description="Type of scraper used")
    city: City = Field(..., description="City scraped")
    pages_scraped: int = Field(default=0, description="Number of pages scraped")
    total_listings: int = Field(default=0, description="Total listings found")
    scrape_date: datetime = Field(..., description="Date and time of scrape")
    storage: StorageResult | None = Field(None, description="Storage operation result")
    error: str | None = Field(None, description="Error message if flow failed")

    model_config = {"use_enum_values": True}
