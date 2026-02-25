"""Date and time utilities."""

from datetime import datetime


def format_date(date: datetime | None = None) -> str:
    """Format date in ISO format (YYYY-MM-DD).

    Used to create partition folders like: dt=2026-02-25

    Args:
        date: DateTime object. If None, uses current date.

    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    if date is None:
        date = datetime.now()
    return date.strftime("%Y-%m-%d")


def format_timestamp(date: datetime | None = None) -> str:
    """Format timestamp in compact format (YYYYMMDD_HHMMSS).

    Used to create unique filenames like: listings_20260225_143022.parquet

    Args:
        date: DateTime object. If None, uses current datetime.

    Returns:
        Formatted timestamp string (YYYYMMDD_HHMMSS)
    """
    if date is None:
        date = datetime.now()
    return date.strftime("%Y%m%d_%H%M%S")
