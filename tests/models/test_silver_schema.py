"""Unit tests for silver_schema SQL generation helpers."""

from real_estate_data_platform.models.silver_schema import (
    BOOLEAN_COLUMNS,
    LOWERCASE_COLUMNS,
    PK_COLUMNS,
    RANGE_VALIDATED_COLUMNS,
    SILVER_COLUMNS,
    SILVER_REGISTRY,
    STRIP_COLUMNS,
    UPDATE_COLUMNS,
    NumericRange,
    Transform,
    UpsertStrategy,
    build_create_table_sql,
    build_upsert_sql,
)


# ---------------------------------------------------------------------------
# SILVER_COLUMNS / SILVER_REGISTRY sync
# ---------------------------------------------------------------------------
class TestSilverColumnsSync:
    """Verify SILVER_COLUMNS (from Pydantic model) stays in sync with the registry."""

    def test_every_model_field_has_a_registry_entry(self):
        registry_data_cols = {c.name for c in SILVER_REGISTRY if c.upsert != UpsertStrategy.MANAGED}
        model_cols = set(SILVER_COLUMNS)
        missing_in_registry = model_cols - registry_data_cols
        assert not missing_in_registry, (
            f"Fields in RentalsListing but not in SILVER_REGISTRY: {missing_in_registry}"
        )

    def test_every_registry_data_col_exists_in_model(self):
        registry_data_cols = {c.name for c in SILVER_REGISTRY if c.upsert != UpsertStrategy.MANAGED}
        model_cols = set(SILVER_COLUMNS)
        missing_in_model = registry_data_cols - model_cols
        assert not missing_in_model, (
            f"Fields in SILVER_REGISTRY but not in RentalsListing: {missing_in_model}"
        )

    def test_silver_columns_is_not_empty(self):
        assert len(SILVER_COLUMNS) > 0

    def test_silver_columns_preserves_model_field_order(self):
        """SILVER_COLUMNS order must follow RentalsListing.model_fields."""
        from real_estate_data_platform.models.listings import RentalsListing

        assert SILVER_COLUMNS == list(RentalsListing.model_fields.keys())


# ---------------------------------------------------------------------------
# Derived lists
# ---------------------------------------------------------------------------
class TestDerivedLists:
    """Tests for the lists derived from SILVER_REGISTRY."""

    def test_pk_columns_are_listing_id_and_website(self):
        assert PK_COLUMNS == ["listing_id", "website"]

    def test_boolean_columns_only_contain_to_boolean_transforms(self):
        for col_name in BOOLEAN_COLUMNS:
            col_def = next(c for c in SILVER_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.TO_BOOLEAN

    def test_strip_columns_only_contain_strip_transforms(self):
        for col_name in STRIP_COLUMNS:
            col_def = next(c for c in SILVER_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.STRIP

    def test_lowercase_columns_only_contain_lowercase_transforms(self):
        for col_name in LOWERCASE_COLUMNS:
            col_def = next(c for c in SILVER_REGISTRY if c.name == col_name)
            assert col_def.transform == Transform.LOWERCASE

    def test_update_columns_exclude_pk_and_managed(self):
        for col_name in UPDATE_COLUMNS:
            col_def = next(c for c in SILVER_REGISTRY if c.name == col_name)
            assert col_def.upsert == UpsertStrategy.OVERWRITE

    def test_update_columns_do_not_contain_pk(self):
        for pk in PK_COLUMNS:
            assert pk not in UPDATE_COLUMNS

    def test_range_validated_columns_all_have_numeric_range(self):
        for _col_name, rng in RANGE_VALIDATED_COLUMNS.items():
            assert isinstance(rng, NumericRange)
            assert rng.min is not None or rng.max is not None


# ---------------------------------------------------------------------------
# build_create_table_sql
# ---------------------------------------------------------------------------
class TestBuildCreateTableSql:
    """Tests for the CREATE TABLE generator."""

    def test_starts_with_create_table_if_not_exists(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        assert sql.startswith("CREATE TABLE IF NOT EXISTS silver.rentals_listings")

    def test_contains_all_registry_columns(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        for col in SILVER_REGISTRY:
            assert col.name in sql

    def test_contains_primary_key_clause(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        pk_csv = ", ".join(PK_COLUMNS)
        assert f"PRIMARY KEY ({pk_csv})" in sql

    def test_not_null_columns_have_constraint(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        not_null_cols = [c for c in SILVER_REGISTRY if not c.nullable]
        for col in not_null_cols:
            # Find the line for this column and check it contains NOT NULL
            lines = sql.split("\n")
            col_line = next(
                line for line in lines if col.name in line and "PRIMARY KEY" not in line
            )
            assert "NOT NULL" in col_line, f"{col.name} should be NOT NULL"

    def test_managed_column_has_default(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        managed = [c for c in SILVER_REGISTRY if c.sql_default]
        for col in managed:
            lines = sql.split("\n")
            col_line = next(
                line for line in lines if col.name in line and "PRIMARY KEY" not in line
            )
            assert f"DEFAULT {col.sql_default}" in col_line

    def test_uses_custom_schema_and_table(self):
        sql = build_create_table_sql("custom_schema", "custom_table")
        assert "custom_schema.custom_table" in sql

    def test_sql_types_match_registry(self):
        sql = build_create_table_sql("silver", "rentals_listings")
        for col in SILVER_REGISTRY:
            # Each column appears as "    col_name   TYPE ..."
            col_lines = [
                line for line in sql.split("\n") if col.name in line and "PRIMARY KEY" not in line
            ]
            combined = " ".join(col_lines)
            assert col.sql_type.value in combined, (
                f"{col.name} should have type {col.sql_type.value}"
            )


# ---------------------------------------------------------------------------
# build_upsert_sql
# ---------------------------------------------------------------------------
class TestBuildUpsertSql:
    """Tests for the INSERT ON CONFLICT DO UPDATE generator."""

    def test_starts_with_insert_into(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        assert sql.startswith("INSERT INTO silver.rentals_listings")

    def test_contains_all_silver_columns(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        for col in SILVER_COLUMNS:
            assert col in sql

    def test_contains_managed_columns(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        managed = [c for c in SILVER_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
        for col in managed:
            assert col.name in sql

    def test_has_correct_number_of_placeholders(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        values_line = next(line for line in sql.split("\n") if line.startswith("VALUES"))
        placeholder_count = values_line.count("%s")
        assert placeholder_count == len(SILVER_COLUMNS)

    def test_on_conflict_uses_pk_columns(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        pk_csv = ", ".join(PK_COLUMNS)
        assert f"ON CONFLICT ({pk_csv})" in sql

    def test_set_clause_contains_update_columns(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        for col in UPDATE_COLUMNS:
            assert f"{col} = EXCLUDED.{col}" in sql

    def test_set_clause_excludes_pk_columns(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        set_section = sql.split("DO UPDATE SET\n")[1]
        for pk in PK_COLUMNS:
            assert f"{pk} = EXCLUDED.{pk}" not in set_section

    def test_managed_columns_use_sql_default_in_set(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        managed = [c for c in SILVER_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
        set_section = sql.split("DO UPDATE SET\n")[1]
        for col in managed:
            assert f"{col.name} = {col.sql_default}" in set_section

    def test_where_guard_references_scraped_at(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        assert "WHERE EXCLUDED.scraped_at > silver.rentals_listings.scraped_at" in sql

    def test_uses_custom_schema_and_table(self):
        sql = build_upsert_sql("custom_schema", "custom_table")
        assert "INSERT INTO custom_schema.custom_table" in sql
        assert "custom_schema.custom_table.scraped_at" in sql

    def test_managed_columns_appear_in_values_as_expressions(self):
        sql = build_upsert_sql("silver", "rentals_listings")
        values_line = next(line for line in sql.split("\n") if line.startswith("VALUES"))
        managed = [c for c in SILVER_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
        for col in managed:
            assert col.sql_default in values_line
