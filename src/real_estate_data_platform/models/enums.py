"""Enumeration types for the real estate data platform."""

from enum import StrEnum


class City(StrEnum):
    """Supported cities for scraping."""

    TORONTO = "toronto"
    VANCOUVER = "vancouver"
    LONDON = "london"


class FlowStatus(StrEnum):
    """Status of a scraping flow execution."""

    SUCCESS = "success"
    ERROR = "error"
    COMPLETED_NO_DATA = "completed_no_data"


class ScraperMode(StrEnum):
    """Mode for scraping data based on date range."""

    LAST_X_DAYS = "last_x_days"
    SPECIFIC_DATE = "specific_date"
