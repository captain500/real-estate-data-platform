"""Date and time utilities."""

from datetime import datetime


def format_partition_date(date: datetime | None = None) -> str:
    """Format date for partition key in object path.

    Used to create partition folders like: dt=2026-02-25

    Args:
        date: DateTime object. If None, uses current date.

    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    if date is None:
        date = datetime.now()
    return date.strftime("%Y-%m-%d")


def format_filename_timestamp(date: datetime | None = None) -> str:
    """Format timestamp for unique filename suffix.

    Used to create unique filenames like: listings_20260225_143022.parquet

    Args:
        date: DateTime object. If None, uses current datetime.

    Returns:
        Formatted timestamp string (YYYYMMDD_HHMMSS)
    """
    if date is None:
        date = datetime.now()
    return date.strftime("%Y%m%d_%H%M%S")
