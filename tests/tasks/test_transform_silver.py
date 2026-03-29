"""Tests for real_estate_data_platform.tasks.transform_silver."""

from unittest.mock import patch

import polars as pl
import pytest

from real_estate_data_platform.models.silver_schema import (
    HASH_COLUMN,
    LISTING_COLUMNS,
    NumericRange,
)
from real_estate_data_platform.tasks.transform_silver import (
    SilverFrames,
    _apply_range,
    _to_boolean,
    transform_to_silver,
)

_PATCH_PREFIX = "real_estate_data_platform.tasks.transform_silver"


def _minimal_bronze_row(**overrides) -> dict:
    """Build a minimal valid bronze row with all expected columns.

    All columns must be present to avoid Polars Null-type issues when the
    transform adds missing columns via ``pl.lit(None)``.
    """
    base: dict = {
        # Core identification
        "listing_id": "123",
        "website": "kijiji",
        "url": "https://kijiji.ca/v/123",
        "published_at": "2026-03-01T10:00:00Z",
        "title": "  Nice Condo  ",
        "description": "A lovely place",
        # Address
        "street": "  123 Main St  ",
        "city": "Toronto",
        "neighbourhood": "Downtown",
        # Pricing
        "rent": 1500.0,
        "move_in_date": "2026-04-01",
        # Property details
        "bedrooms": 2,
        "bathrooms": 1,
        "size_sqft": 800.0,
        "unit_type": "Condo",
        "agreement_type": "1 year",
        "furnished": "Yes",
        "for_rent_by": "Owner",
        # Location
        "latitude": 43.65,
        "longitude": -79.38,
        "walk_score": 8.5,
        "transit_score": 7.0,
        "bike_score": 6.0,
        # Media
        "images": ["img1.jpg"],
        # Amenities — Infrastructure
        "elevator": "No",
        "gym": "No",
        "concierge": "No",
        "security_24h": "No",
        "pool": "No",
        # Amenities — Living
        "balcony": "Yes",
        "yard": "No",
        "storage_space": "No",
        # Amenities — Utilities
        "heat": "Included",
        "water": "Not Included",
        "hydro": "No",
        "internet": "No",
        "cable_tv": "No",
        # Amenities — Laundry & Parking
        "laundry_in_unit": "No",
        "laundry_in_building": "Yes",
        "parking_included": 0,
        # Amenities — Kitchen
        "dishwasher": "No",
        "fridge_freezer": "Yes",
        # Amenities — Pet & Accessibility
        "pet_friendly": "no",
        "smoking_permitted": "no",
        "wheelchair_accessible": "No",
        "barrier_free": "No",
        "accessible_washrooms": "No",
        "audio_prompts": "No",
        "visual_aids": "No",
        "braille_labels": "No",
        # Amenities — Other
        "bicycle_parking": "No",
        "air_conditioning": "No",
        # Metadata
        "scraped_at": "2026-03-29T12:00:00Z",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _to_boolean
# ---------------------------------------------------------------------------
class TestToBoolean:
    """Tests for the boolean expression builder."""

    @pytest.mark.parametrize("raw", ["Yes", "yes", "  YES  ", "included", "Included"])
    def test_truthy_values(self, raw):
        df = pl.DataFrame({"col": [raw]})
        result = df.select(_to_boolean("col"))["col"][0]
        assert result is True

    @pytest.mark.parametrize("raw", ["No", "no", "  NO  ", "not included", "Not Included"])
    def test_falsy_values(self, raw):
        df = pl.DataFrame({"col": [raw]})
        result = df.select(_to_boolean("col"))["col"][0]
        assert result is False

    @pytest.mark.parametrize("raw", ["maybe", "N/A", ""])
    def test_unrecognised_returns_none(self, raw):
        df = pl.DataFrame({"col": [raw]})
        result = df.select(_to_boolean("col"))["col"][0]
        assert result is None

    def test_null_returns_none(self):
        df = pl.DataFrame({"col": [None]}, schema={"col": pl.Utf8})
        result = df.select(_to_boolean("col"))["col"][0]
        assert result is None


# ---------------------------------------------------------------------------
# _apply_range
# ---------------------------------------------------------------------------
class TestApplyRange:
    """Tests for range validation expression builder."""

    def test_value_within_range_kept(self):
        df = pl.DataFrame({"x": [5.0]})
        result = df.select(_apply_range("x", NumericRange(min=0, max=10)))["x"][0]
        assert result == 5.0

    def test_value_below_min_nulled(self):
        df = pl.DataFrame({"x": [-1.0]})
        result = df.select(_apply_range("x", NumericRange(min=0, max=10)))["x"][0]
        assert result is None

    def test_value_above_max_nulled(self):
        df = pl.DataFrame({"x": [15.0]})
        result = df.select(_apply_range("x", NumericRange(min=0, max=10)))["x"][0]
        assert result is None

    def test_exclusive_min(self):
        """With exclusive=True, the min boundary itself is rejected."""
        df = pl.DataFrame({"x": [0.0, 0.01]})
        result = df.select(_apply_range("x", NumericRange(min=0, exclusive=True)))["x"].to_list()
        assert result[0] is None  # 0.0 excluded
        assert result[1] == pytest.approx(0.01)

    def test_min_only(self):
        df = pl.DataFrame({"x": [-1.0, 0.0, 100.0]})
        result = df.select(_apply_range("x", NumericRange(min=0)))["x"].to_list()
        assert result[0] is None
        assert result[1] == 0.0
        assert result[2] == 100.0

    def test_max_only(self):
        df = pl.DataFrame({"x": [5.0, 10.0, 11.0]})
        result = df.select(_apply_range("x", NumericRange(max=10)))["x"].to_list()
        assert result[0] == 5.0
        assert result[1] == 10.0
        assert result[2] is None

    def test_null_passes_through(self):
        df = pl.DataFrame({"x": [None]}, schema={"x": pl.Float64})
        result = df.select(_apply_range("x", NumericRange(min=0, max=10)))["x"][0]
        assert result is None


# ---------------------------------------------------------------------------
# transform_to_silver (full pipeline, called via .fn() to bypass Prefect)
# ---------------------------------------------------------------------------
@patch(f"{_PATCH_PREFIX}.get_run_logger")
class TestTransformToSilver:
    """Integration tests for the core silver transform."""

    def _run(self, rows: list[dict]) -> SilverFrames:
        """Helper: build a DataFrame and run the transform (bypassing Prefect).

        Casts Null-typed columns to Utf8 to match real Parquet behaviour.
        """
        df = pl.DataFrame(rows)
        df = df.cast(
            {c: pl.Utf8 for c, t in zip(df.columns, df.dtypes, strict=False) if t == pl.Null}
        )
        return transform_to_silver.fn(df)

    def test_basic_transform_produces_both_frames(self, _mock_logger):
        result = self._run([_minimal_bronze_row()])
        assert isinstance(result, SilverFrames)
        assert result.listings.height == 1
        assert result.neighbourhoods.height == 1

    def test_strips_whitespace_from_title_and_street(self, _mock_logger):
        result = self._run([_minimal_bronze_row(title="  Condo  ", street=" 123 Main ")])
        row = result.listings.row(0, named=True)
        assert row["title"] == "Condo"
        assert row["street"] == "123 Main"

    def test_lowercases_city_and_unit_type(self, _mock_logger):
        result = self._run([_minimal_bronze_row(city="TORONTO", unit_type="CONDO")])
        row = result.listings.row(0, named=True)
        assert row["city"] == "toronto"
        assert row["unit_type"] == "condo"

    def test_boolean_conversion(self, _mock_logger):
        result = self._run([_minimal_bronze_row(furnished="Yes", heat="Not Included")])
        row = result.listings.row(0, named=True)
        assert row["furnished"] is True
        assert row["heat"] is False

    def test_drops_rows_with_null_pk(self, _mock_logger):
        rows = [
            _minimal_bronze_row(listing_id="good"),
            _minimal_bronze_row(listing_id=None),
            _minimal_bronze_row(listing_id=""),
        ]
        result = self._run(rows)
        assert result.listings.height == 1
        assert result.listings["listing_id"][0] == "good"

    def test_dedup_keeps_latest_scraped_at(self, _mock_logger):
        rows = [
            _minimal_bronze_row(listing_id="dup", scraped_at="2026-03-28T10:00:00Z", rent=1000.0),
            _minimal_bronze_row(listing_id="dup", scraped_at="2026-03-29T10:00:00Z", rent=1200.0),
        ]
        result = self._run(rows)
        assert result.listings.height == 1
        assert result.listings["rent"][0] == 1200.0

    def test_invalid_rent_nulled(self, _mock_logger):
        result = self._run([_minimal_bronze_row(rent=-500.0)])
        assert result.listings["rent"][0] is None

    def test_invalid_latitude_nulled(self, _mock_logger):
        result = self._run([_minimal_bronze_row(latitude=200.0)])
        assert result.listings["latitude"][0] is None

    def test_valid_latitude_kept(self, _mock_logger):
        result = self._run([_minimal_bronze_row(latitude=43.65)])
        assert result.listings["latitude"][0] == pytest.approx(43.65)

    def test_row_hash_column_present(self, _mock_logger):
        result = self._run([_minimal_bronze_row()])
        assert HASH_COLUMN in result.listings.columns
        h = result.listings[HASH_COLUMN][0]
        assert isinstance(h, str)
        assert len(h) == 32

    def test_output_columns_match_schema(self, _mock_logger):
        result = self._run([_minimal_bronze_row()])
        assert set(result.listings.columns) == set(LISTING_COLUMNS)

    def test_neighbourhood_extraction(self, _mock_logger):
        result = self._run(
            [
                _minimal_bronze_row(neighbourhood="Downtown", city="Toronto"),
                _minimal_bronze_row(listing_id="456", neighbourhood="Downtown", city="Toronto"),
            ]
        )
        # Should deduplicate by neighbourhood PK
        assert result.neighbourhoods.height == 1
        row = result.neighbourhoods.row(0, named=True)
        assert row["neighbourhood"] == "Downtown"
