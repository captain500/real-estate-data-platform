"""Tests for real_estate_data_platform.utils.parsers."""

import pytest

from real_estate_data_platform.utils.parsers import parse_float, parse_int


# ---------------------------------------------------------------------------
# parse_float
# ---------------------------------------------------------------------------
class TestParseFloat:
    """Tests for parse_float — extracts floats from pricing/currency strings."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("1500", 1500.0),
            ("1,500.50", 1500.50),
            ("$2,100", 2100.0),
            ("$2,100.75", 2100.75),
            ("  $1,200  ", 1200.0),
            ("0", 0.0),
            ("3.14", 3.14),
        ],
    )
    def test_valid_values(self, raw, expected):
        assert parse_float(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_empty_or_none_returns_none(self, raw):
        assert parse_float(raw) is None

    @pytest.mark.parametrize("raw", ["abc", "N/A", "not available"])
    def test_non_numeric_returns_none(self, raw):
        assert parse_float(raw) is None


# ---------------------------------------------------------------------------
# parse_int
# ---------------------------------------------------------------------------
class TestParseInt:
    """Tests for parse_int — extracts integers from strings with mixed content."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("3", 3),
            ("42 bedrooms", 42),
            ("1,200 sqft", 1200),
            ("Unit #5", 5),
        ],
    )
    def test_valid_values(self, raw, expected):
        assert parse_int(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_empty_or_none_returns_none(self, raw):
        assert parse_int(raw) is None

    def test_no_digits_returns_none(self):
        assert parse_int("no digits here") is None
