"""Tests for real_estate_data_platform.utils.dates."""

from datetime import UTC, date, datetime, timedelta

import pytest

from real_estate_data_platform.utils.dates import date_range, format_date, parse_iso_datetime


# ---------------------------------------------------------------------------
# parse_iso_datetime
# ---------------------------------------------------------------------------
class TestParseIsoDatetime:
    """Tests for ISO 8601 datetime string parsing."""

    def test_standard_iso_with_z_suffix(self):
        dt = parse_iso_datetime("2026-02-12T08:03:15.000Z")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 2
        assert dt.day == 12

    def test_standard_iso_with_offset(self):
        dt = parse_iso_datetime("2026-03-01T10:00:00+00:00")
        assert dt is not None
        assert dt.hour == 10

    def test_date_only_string(self):
        dt = parse_iso_datetime("2026-03-15")
        assert dt is not None
        assert dt.year == 2026

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_empty_or_none_returns_none(self, value):
        assert parse_iso_datetime(value) is None

    def test_invalid_format_returns_none(self):
        assert parse_iso_datetime("not-a-date") is None

    def test_garbage_returns_none(self):
        assert parse_iso_datetime("12/31/2026 pizza") is None


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------
class TestFormatDate:
    """Tests for date formatting (partition paths)."""

    def test_with_date_object(self):
        assert format_date(date(2026, 1, 15)) == "2026-01-15"

    def test_with_datetime_object(self):
        dt = datetime(2026, 12, 25, 14, 30, tzinfo=UTC)
        assert format_date(dt) == "2026-12-25"

    def test_defaults_to_today(self):
        result = format_date()
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        assert result == today

    def test_zero_padded(self):
        assert format_date(date(2026, 3, 5)) == "2026-03-05"


# ---------------------------------------------------------------------------
# date_range
# ---------------------------------------------------------------------------
class TestDateRange:
    """Tests for date_range — generates partition date lists."""

    def test_single_day_returns_today(self):
        result = date_range(1)
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        assert result == [today]

    def test_returns_correct_number_of_days(self):
        result = date_range(7)
        assert len(result) == 7

    def test_most_recent_first(self):
        result = date_range(3)
        dates = [datetime.strptime(d, "%Y-%m-%d") for d in result]
        assert dates == sorted(dates, reverse=True)

    def test_consecutive_dates(self):
        result = date_range(5)
        dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in result]
        for i in range(len(dates) - 1):
            assert dates[i] - dates[i + 1] == timedelta(days=1)
