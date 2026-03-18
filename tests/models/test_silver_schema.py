"""Unit tests for silver_schema column registries and SQL generation helpers."""

from real_estate_data_platform.models.silver_schema import (
    BOOLEAN_COLUMNS,
    HASH_COLUMN,
    HASH_COLUMNS,
    INPUT_COLUMNS,
    LISTING_COLUMNS,
    LISTING_PK_COLUMNS,
    LISTINGS_REGISTRY,
    LOWERCASE_COLUMNS,
    NEIGHBOURHOOD_COLUMNS,
    NEIGHBOURHOOD_PK_COLUMNS,
    NEIGHBOURHOOD_REGISTRY,
    NEIGHBOURHOOD_UPDATE_COLUMNS,
    RANGE_VALIDATED_COLUMNS,
    STRIP_COLUMNS,
    NumericRange,
    Transform,
    UpsertStrategy,
    build_create_table_sql,
    build_listings_upsert_sql,
    build_neighbourhood_upsert_sql,
)


# ---------------------------------------------------------------------------
# INPUT_COLUMNS / registry sync
# ---------------------------------------------------------------------------
class TestInputColumnsSync:
    """Verify INPUT_COLUMNS (from Pydantic model) stays in sync with the registries."""

    def test_every_model_field_covered_by_a_registry(self):
        listing_data_cols = {
            c.name for c in LISTINGS_REGISTRY if c.upsert != UpsertStrategy.MANAGED
        }
        neighbourhood_only_cols = {
            c.name for c in NEIGHBOURHOOD_REGISTRY if c.upsert != UpsertStrategy.MANAGED
        }
        # Computed listing columns not present in the scraping model.
        computed = {HASH_COLUMN}
        all_registry_cols = (listing_data_cols | neighbourhood_only_cols) - computed
        model_cols = set(INPUT_COLUMNS)
        missing = model_cols - all_registry_cols
        assert not missing, f"Fields in RentalsListing but not in any registry: {missing}"

    def test_input_columns_is_not_empty(self):
        assert len(INPUT_COLUMNS) > 0

    def test_input_columns_preserves_model_field_order(self):
        """INPUT_COLUMNS order must follow RentalsListing.model_fields."""
        from real_estate_data_platform.models.listings import RentalsListing

        assert INPUT_COLUMNS == list(RentalsListing.model_fields.keys())


# ---------------------------------------------------------------------------
# Derived lists — listings
# ---------------------------------------------------------------------------
class TestListingsDerivedLists:
    """Tests for lists derived from LISTINGS_REGISTRY."""

    def test_pk_columns_are_listing_id_and_website(self):
        assert LISTING_PK_COLUMNS == ["listing_id", "website"]

    def test_boolean_columns_only_contain_to_boolean_transforms(self):
        for col_name in BOOLEAN_COLUMNS:
            col_def = next(c for c in LISTINGS_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.TO_BOOLEAN

    def test_strip_columns_only_contain_strip_transforms(self):
        for col_name in STRIP_COLUMNS:
            col_def = next(c for c in LISTINGS_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.STRIP

    def test_lowercase_columns_only_contain_lowercase_transforms(self):
        for col_name in LOWERCASE_COLUMNS:
            col_def = next(c for c in LISTINGS_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.LOWERCASE

    def test_listing_columns_exclude_managed(self):
        for col_name in LISTING_COLUMNS:
            col_def = next(c for c in LISTINGS_REGISTRY if c.name == col_name)
            assert col_def.upsert != UpsertStrategy.MANAGED

    def test_listing_columns_do_not_contain_scores(self):
        for score in ("walk_score", "transit_score", "bike_score"):
            assert score not in LISTING_COLUMNS

    def test_hash_columns_only_contain_hashed_fields(self):
        for col_name in HASH_COLUMNS:
            col_def = next(c for c in LISTINGS_REGISTRY if c.name == col_name)
            assert col_def.hashed

    def test_hash_columns_is_not_empty(self):
        assert len(HASH_COLUMNS) > 0

    def test_range_validated_columns_all_have_numeric_range(self):
        for _col_name, rng in RANGE_VALIDATED_COLUMNS.items():
            assert isinstance(rng, NumericRange)
            assert rng.min is not None or rng.max is not None


# ---------------------------------------------------------------------------
# Derived lists — neighbourhoods
# ---------------------------------------------------------------------------
class TestNeighbourhoodDerivedLists:
    """Tests for lists derived from NEIGHBOURHOOD_REGISTRY."""

    def test_pk_columns_are_neighbourhood_and_city(self):
        assert NEIGHBOURHOOD_PK_COLUMNS == ["neighbourhood", "city"]

    def test_columns_exclude_managed(self):
        for col_name in NEIGHBOURHOOD_COLUMNS:
            col_def = next(c for c in NEIGHBOURHOOD_REGISTRY if c.name == col_name)
            assert col_def.upsert != UpsertStrategy.MANAGED

    def test_update_columns_are_overwrite_only(self):
        for col_name in NEIGHBOURHOOD_UPDATE_COLUMNS:
            col_def = next(c for c in NEIGHBOURHOOD_REGISTRY if c.name == col_name)
            assert col_def.upsert == UpsertStrategy.OVERWRITE

    def test_scores_are_in_neighbourhood_columns(self):
        for score in ("walk_score", "transit_score", "bike_score"):
            assert score in NEIGHBOURHOOD_COLUMNS


# ---------------------------------------------------------------------------
# build_create_table_sql
# ---------------------------------------------------------------------------
class TestBuildCreateTableSql:
    """Tests for the CREATE TABLE generator."""

    def test_listings_starts_with_create_table(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        assert sql.startswith("CREATE TABLE IF NOT EXISTS silver.rentals_listings")

    def test_listings_contains_all_registry_columns(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        for col in LISTINGS_REGISTRY:
            assert col.name in sql

    def test_listings_contains_primary_key_clause(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        pk_csv = ", ".join(LISTING_PK_COLUMNS)
        assert f"PRIMARY KEY ({pk_csv})" in sql

    def test_listings_not_null_columns_have_constraint(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        not_null_cols = [c for c in LISTINGS_REGISTRY if not c.nullable]
        for col in not_null_cols:
            lines = sql.split("\n")
            col_line = next(
                line for line in lines if col.name in line and "PRIMARY KEY" not in line
            )
            assert "NOT NULL" in col_line, f"{col.name} should be NOT NULL"

    def test_listings_managed_column_has_default(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        managed = [c for c in LISTINGS_REGISTRY if c.sql_default]
        for col in managed:
            lines = sql.split("\n")
            col_line = next(
                line for line in lines if col.name in line and "PRIMARY KEY" not in line
            )
            assert f"DEFAULT {col.sql_default}" in col_line

    def test_neighbourhoods_starts_with_create_table(self):
        sql = build_create_table_sql(NEIGHBOURHOOD_REGISTRY, "silver", "neighbourhoods")
        assert sql.startswith("CREATE TABLE IF NOT EXISTS silver.neighbourhoods")

    def test_neighbourhoods_contains_all_registry_columns(self):
        sql = build_create_table_sql(NEIGHBOURHOOD_REGISTRY, "silver", "neighbourhoods")
        for col in NEIGHBOURHOOD_REGISTRY:
            assert col.name in sql

    def test_uses_custom_schema_and_table(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "custom_schema", "custom_table")
        assert "custom_schema.custom_table" in sql

    def test_sql_types_match_registry(self):
        sql = build_create_table_sql(LISTINGS_REGISTRY, "silver", "rentals_listings")
        for col in LISTINGS_REGISTRY:
            col_lines = [
                line for line in sql.split("\n") if col.name in line and "PRIMARY KEY" not in line
            ]
            combined = " ".join(col_lines)
            assert col.sql_type.value in combined, (
                f"{col.name} should have type {col.sql_type.value}"
            )


# ---------------------------------------------------------------------------
# build_neighbourhood_upsert_sql
# ---------------------------------------------------------------------------
class TestBuildNeighbourhoodUpsertSql:
    """Tests for the neighbourhoods INSERT ON CONFLICT generator."""

    def test_starts_with_insert_into(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        assert sql.startswith("INSERT INTO silver.neighbourhoods")

    def test_contains_all_neighbourhood_columns(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        for col in NEIGHBOURHOOD_COLUMNS:
            assert col in sql

    def test_has_correct_number_of_placeholders(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        values_line = next(line for line in sql.split("\n") if line.startswith("VALUES"))
        placeholder_count = values_line.count("%s")
        assert placeholder_count == len(NEIGHBOURHOOD_COLUMNS)

    def test_on_conflict_uses_pk_columns(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        pk_csv = ", ".join(NEIGHBOURHOOD_PK_COLUMNS)
        assert f"ON CONFLICT ({pk_csv})" in sql

    def test_set_clause_contains_update_columns(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        for col in NEIGHBOURHOOD_UPDATE_COLUMNS:
            assert f"{col} = EXCLUDED.{col}" in sql

    def test_managed_columns_use_sql_default_in_set(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        managed = [c for c in NEIGHBOURHOOD_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
        set_section = sql.split("DO UPDATE SET\n")[1]
        for col in managed:
            assert f"{col.name} = {col.sql_default}" in set_section

    def test_no_where_guard(self):
        sql = build_neighbourhood_upsert_sql("silver", "neighbourhoods")
        assert "WHERE" not in sql


# ---------------------------------------------------------------------------
# build_listings_upsert_sql
# ---------------------------------------------------------------------------
class TestBuildListingsUpsertSql:
    """Tests for the hash-aware listings INSERT ON CONFLICT generator."""

    def test_starts_with_insert_into(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        assert sql.startswith("INSERT INTO silver.rentals_listings")

    def test_contains_all_listing_columns(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        for col in LISTING_COLUMNS:
            assert col in sql

    def test_has_correct_number_of_placeholders(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        values_line = next(line for line in sql.split("\n") if line.startswith("VALUES"))
        placeholder_count = values_line.count("%s")
        assert placeholder_count == len(LISTING_COLUMNS)

    def test_on_conflict_uses_listing_pk(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        pk_csv = ", ".join(LISTING_PK_COLUMNS)
        assert f"ON CONFLICT ({pk_csv})" in sql

    def test_always_update_columns_are_unconditional(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        always = [c.name for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.ALWAYS_UPDATE]
        set_section = sql.split("DO UPDATE SET\n")[1]
        for col in always:
            assert f"{col} = EXCLUDED.{col}" in set_section

    def test_overwrite_columns_are_conditional_on_hash(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        overwrite = [c.name for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.OVERWRITE]
        for col in overwrite:
            assert f"EXCLUDED.{HASH_COLUMN} != silver.rentals_listings.{HASH_COLUMN}" in sql
            assert f"THEN EXCLUDED.{col}" in sql

    def test_managed_columns_conditional_on_hash(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        managed = [c for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
        for col in managed:
            assert f"THEN {col.sql_default}" in sql

    def test_where_guard_uses_scraped_at(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        assert "WHERE EXCLUDED.scraped_at >= silver.rentals_listings.scraped_at" in sql

    def test_uses_custom_schema_and_table(self):
        sql = build_listings_upsert_sql("custom_schema", "custom_table")
        assert "INSERT INTO custom_schema.custom_table" in sql
        assert "custom_schema.custom_table.scraped_at" in sql

    def test_no_sql_expression_columns_in_listings(self):
        sql = build_listings_upsert_sql("silver", "rentals_listings")
        values_line = next(line for line in sql.split("\n") if line.startswith("VALUES"))
        assert "NOW()" not in values_line
