"""Date and time utilities."""

from datetime import UTC, date, datetime, timedelta


def parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string (e.g., '2026-02-12T08:03:15.000Z').

    Args:
        value: ISO datetime string, optionally with 'Z' suffix.

    Returns:
        Parsed datetime or None if parsing fails.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def format_date(dt: date | datetime | None = None) -> str:
    """Format date in ISO format (YYYY-MM-DD).

    Used to create partition folders like: dt=2026-02-25

    Args:
        dt: Date or DateTime object. If None, uses current date.

    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    if dt is None:
        dt = datetime.now(UTC)
    return dt.strftime("%Y-%m-%d")


def date_range(days: int) -> list[str]:
    """Generate a list of date strings for the last N days (including today).

    Args:
        days: Number of days to look back (1 = today only)

    Returns:
        List of date strings (YYYY-MM-DD), most recent first
    """
    today = datetime.now(UTC)
    return [format_date(today - timedelta(days=d)) for d in range(days)]
