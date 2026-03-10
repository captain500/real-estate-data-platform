"""Centralised column registry for the silver layer.

TODO: Split into listings_current (latest state + first_seen_at, last_seen_at,
is_active) and listings_price_history (new row only when price changes).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto


# Enums
class SqlType(StrEnum):
    """PostgreSQL data types used in the silver schema."""

    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE PRECISION"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TEXT_ARRAY = "TEXT[]"


class Transform(StrEnum):
    """Transform to apply during bronze → silver cleaning."""

    NONE = auto()
    TO_BOOLEAN = auto()  # "Yes"/"Included" → True, "No"/"Not Included" → False
    STRIP = auto()  # strip whitespace only
    LOWERCASE = auto()  # strip whitespace + lowercase


class UpsertStrategy(StrEnum):
    """How the column behaves on ``INSERT … ON CONFLICT DO UPDATE``."""

    OVERWRITE = auto()  # SET col = EXCLUDED.col  (default)
    SKIP = auto()  # Primary-key columns — never in SET clause
    MANAGED = auto()  # Managed by a SQL expression (e.g. loaded_at = NOW())


# Numeric range validation
@dataclass(frozen=True, slots=True)
class NumericRange:
    """Valid numeric range for a column. Values outside are nulled during transform."""

    min: float | None = None
    max: float | None = None
    exclusive: bool = False  # True → strict inequality for min bound (> instead of >=)


# Column definition
@dataclass(frozen=True, slots=True)
class ColumnDef:
    """Definition of a single silver-layer column."""

    name: str
    sql_type: SqlType
    nullable: bool = True
    is_pk: bool = False
    transform: Transform = Transform.NONE
    upsert: UpsertStrategy = UpsertStrategy.OVERWRITE
    sql_default: str | None = None  # e.g. "NOW()" for loaded_at
    valid_range: NumericRange | None = None  # null out values outside this range


# Column registry
SILVER_REGISTRY: tuple[ColumnDef, ...] = (
    # ── Core identification ──────────────────────────────────────────────
    ColumnDef("listing_id", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP),
    ColumnDef("website", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP),
    ColumnDef("url", SqlType.TEXT, nullable=False),
    ColumnDef("published_at", SqlType.TIMESTAMPTZ, nullable=False),
    ColumnDef("title", SqlType.TEXT, nullable=False, transform=Transform.STRIP),
    ColumnDef("description", SqlType.TEXT, nullable=False, transform=Transform.STRIP),
    # ── Address ──────────────────────────────────────────────────────────
    ColumnDef("street", SqlType.TEXT, nullable=False, transform=Transform.STRIP),
    ColumnDef("city", SqlType.TEXT, nullable=False, transform=Transform.LOWERCASE),
    ColumnDef("neighbourhood", SqlType.TEXT, transform=Transform.STRIP),
    # ── Pricing & dates ──────────────────────────────────────────────────
    ColumnDef("rent", SqlType.DOUBLE, valid_range=NumericRange(min=0, exclusive=True)),
    ColumnDef("move_in_date", SqlType.TEXT),
    # ── Property details ─────────────────────────────────────────────────
    ColumnDef("bedrooms", SqlType.INTEGER),
    ColumnDef("bathrooms", SqlType.INTEGER),
    ColumnDef("size_sqft", SqlType.DOUBLE),
    ColumnDef("unit_type", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("agreement_type", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("furnished", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("for_rent_by", SqlType.TEXT, transform=Transform.LOWERCASE),
    # ── Location ─────────────────────────────────────────────────────────
    ColumnDef("latitude", SqlType.DOUBLE, valid_range=NumericRange(min=41.0, max=84.0)),
    ColumnDef("longitude", SqlType.DOUBLE, valid_range=NumericRange(min=-142.0, max=-52.0)),
    ColumnDef("walk_score", SqlType.DOUBLE),
    ColumnDef("transit_score", SqlType.DOUBLE),
    ColumnDef("bike_score", SqlType.DOUBLE),
    # ── Media ────────────────────────────────────────────────────────────
    ColumnDef("images", SqlType.TEXT_ARRAY),
    # ── Amenities — Infrastructure ───────────────────────────────────────
    ColumnDef("elevator", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("gym", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("concierge", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("security_24h", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("pool", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Amenities — Living features ──────────────────────────────────────
    ColumnDef("balcony", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("yard", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("storage_space", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Amenities — Utilities ────────────────────────────────────────────
    ColumnDef("heat", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("water", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("hydro", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("internet", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("cable_tv", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Amenities — Laundry & Parking ────────────────────────────────────
    ColumnDef("laundry_in_unit", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("laundry_in_building", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("parking_included", SqlType.INTEGER, valid_range=NumericRange(min=0)),
    # ── Amenities — Kitchen ──────────────────────────────────────────────
    ColumnDef("dishwasher", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("fridge_freezer", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Amenities — Pet & Accessibility ──────────────────────────────────
    ColumnDef("pet_friendly", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("smoking_permitted", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("wheelchair_accessible", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("barrier_free", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("accessible_washrooms", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("audio_prompts", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("visual_aids", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("braille_labels", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Amenities — Other ────────────────────────────────────────────────
    ColumnDef("bicycle_parking", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    ColumnDef("air_conditioning", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN),
    # ── Metadata ─────────────────────────────────────────────────────────
    ColumnDef("scraped_at", SqlType.TIMESTAMPTZ, nullable=False),
    ColumnDef(
        "loaded_at",
        SqlType.TIMESTAMPTZ,
        nullable=False,
        upsert=UpsertStrategy.MANAGED,
        sql_default="NOW()",
    ),
)


# DataFrame / Parquet columns — everything except SQL-managed (e.g. loaded_at).
SILVER_COLUMNS: list[str] = [c.name for c in SILVER_REGISTRY if c.upsert != UpsertStrategy.MANAGED]

# Columns that need "Yes"/"No" → Boolean conversion.
BOOLEAN_COLUMNS: list[str] = [
    c.name for c in SILVER_REGISTRY if c.transform == Transform.TO_BOOLEAN
]

# Columns to strip whitespace only.
STRIP_COLUMNS: list[str] = [c.name for c in SILVER_REGISTRY if c.transform == Transform.STRIP]

# Columns to strip + lowercase (categorical / enum-like values).
LOWERCASE_COLUMNS: list[str] = [
    c.name for c in SILVER_REGISTRY if c.transform == Transform.LOWERCASE
]

# Primary-key column names.
PK_COLUMNS: list[str] = [c.name for c in SILVER_REGISTRY if c.is_pk]

# Columns with numeric range validation: name → NumericRange.
RANGE_VALIDATED_COLUMNS: dict[str, NumericRange] = {
    c.name: c.valid_range for c in SILVER_REGISTRY if c.valid_range is not None
}

# Column used for temporal ordering during deduplication.
DEDUP_SORT_COLUMN: str = "scraped_at"

# Columns overwritten on upsert conflict.
UPDATE_COLUMNS: list[str] = [
    c.name for c in SILVER_REGISTRY if c.upsert == UpsertStrategy.OVERWRITE
]


# SQL generation helpers
def build_create_table_sql(schema: str, table: str) -> str:
    """Generate a ``CREATE TABLE IF NOT EXISTS`` statement from the registry.

    Args:
        schema: PostgreSQL schema name. Defaults to "silver"
        table: Table name inside schema. Defaults to "rentals_listings"

    Returns:
        SQL statement to create the table with all columns and constraints defined in the registry.
    """
    lines: list[str] = []
    for col in SILVER_REGISTRY:
        parts = [f"    {col.name:<25s} {col.sql_type.value}"]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.sql_default:
            parts.append(f"DEFAULT {col.sql_default}")
        lines.append(" ".join(parts))

    pk_csv = ", ".join(PK_COLUMNS)
    lines.append(f"\n    PRIMARY KEY ({pk_csv})")
    body = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{body}\n)"


def build_upsert_sql(schema: str, table: str) -> str:
    """Generate an ``INSERT ON CONFLICT DO UPDATE`` statement from the registry.

    Uses positional ``%s`` placeholders (one per INSERT_COLUMN) so the caller
    can pass rows as tuples instead of dicts.

    The generated statement includes a ``WHERE`` guard so that an existing row
    is only updated when the incoming ``scraped_at`` is strictly newer than
    the value already stored.  This makes the upsert idempotent and safe for
    reprocessing older partitions without regressing column values.
    """
    qualified = f"{schema}.{table}"
    cols_csv = ", ".join(SILVER_COLUMNS)
    placeholders = ", ".join("%s" for _ in SILVER_COLUMNS)

    # Managed columns added as SQL expressions in VALUES.
    managed = [c for c in SILVER_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
    managed_cols = ", ".join(c.name for c in managed)
    managed_vals = ", ".join(c.sql_default or "NULL" for c in managed)

    all_cols = f"{cols_csv}, {managed_cols}" if managed_cols else cols_csv
    all_vals = f"{placeholders}, {managed_vals}" if managed_vals else placeholders

    # SET clause.
    set_parts: list[str] = [f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS]
    for col_def in managed:
        set_parts.append(f"{col_def.name} = {col_def.sql_default}")

    pk_csv = ", ".join(PK_COLUMNS)
    set_clause = ",\n    ".join(set_parts)

    # WHERE guard: only update when incoming data is newer.
    where_clause = f"WHERE EXCLUDED.scraped_at > {qualified}.scraped_at"

    return (
        f"INSERT INTO {qualified} ({all_cols})\n"
        f"VALUES ({all_vals})\n"
        f"ON CONFLICT ({pk_csv}) DO UPDATE SET\n"
        f"    {set_clause}\n"
        f"{where_clause}"
    )
