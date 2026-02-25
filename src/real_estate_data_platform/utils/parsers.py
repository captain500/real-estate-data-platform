"""Utility functions for parsing and type conversions."""


def parse_float(value: str | None) -> float | None:
    """Extract float value from text.

    Args:
        value: String value potentially containing non-numeric characters

    Returns:
        Parsed float value or None if parsing fails
    """
    if not value:
        return None
    try:
        return float(value.replace(",", "").replace("$", "").strip())
    except (ValueError, AttributeError):
        return None


def parse_int(value: str | None) -> int | None:
    """Extract integer value from text.

    Args:
        value: String value potentially containing non-numeric characters

    Returns:
        Parsed integer value or None if parsing fails
    """
    if not value:
        return None
    try:
        return int("".join(filter(str.isdigit, value)))
    except (ValueError, AttributeError):
        return None
