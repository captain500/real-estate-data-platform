"""Centralised column registry for the silver layer.

Two normalised tables:

- ``rentals_listings``: current state of each listing with hash-based change detection.
- ``neighbourhoods``: unique (neighbourhood, city) pairs with walkability scores.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto

# ── Enums ────────────────────────────────────────────────────────────────────


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

    OVERWRITE = auto()  # SET col = EXCLUDED.col (conditional on hash in listings)
    SKIP = auto()  # Primary-key columns — never in SET clause
    INSERT_ONLY = auto()  # SQL expression on INSERT; excluded from SET (e.g. first_seen_at)
    ALWAYS_UPDATE = auto()  # Always SET col = EXCLUDED.col regardless of hash
    MANAGED = auto()  # SQL expression, included in SET (e.g. loaded_at = NOW())


# ── Numeric range validation ─────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class NumericRange:
    """Valid numeric range for a column. Values outside are nulled during transform."""

    min: float | None = None
    max: float | None = None
    exclusive: bool = False  # True → strict inequality for min bound (> instead of >=)


# ── Column definition ────────────────────────────────────────────────────────


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
    hashed: bool = False  # True → included in the row hash for change detection


# ═══════════════════════════════════════════════════════════════════════════════
# Column registries
# ═══════════════════════════════════════════════════════════════════════════════


NEIGHBOURHOOD_REGISTRY: tuple[ColumnDef, ...] = (
    ColumnDef(
        "neighbourhood", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP
    ),
    ColumnDef("city", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP),
    ColumnDef("walk_score", SqlType.DOUBLE, valid_range=NumericRange(min=0, max=10)),
    ColumnDef("transit_score", SqlType.DOUBLE, valid_range=NumericRange(min=0, max=10)),
    ColumnDef("bike_score", SqlType.DOUBLE, valid_range=NumericRange(min=0, max=10)),
    ColumnDef(
        "loaded_at",
        SqlType.TIMESTAMPTZ,
        nullable=False,
        upsert=UpsertStrategy.MANAGED,
        sql_default="NOW()",
    ),
)


LISTINGS_REGISTRY: tuple[ColumnDef, ...] = (
    # ── Core identification ──────────────────────────────────────────────
    ColumnDef("listing_id", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP),
    ColumnDef("website", SqlType.TEXT, nullable=False, is_pk=True, upsert=UpsertStrategy.SKIP),
    ColumnDef("url", SqlType.TEXT, nullable=False),
    ColumnDef("published_at", SqlType.TIMESTAMPTZ, nullable=False),
    ColumnDef("title", SqlType.TEXT, nullable=False, transform=Transform.STRIP, hashed=True),
    ColumnDef("description", SqlType.TEXT, nullable=False, transform=Transform.STRIP, hashed=True),
    # ── Address ──────────────────────────────────────────────────────────
    ColumnDef("street", SqlType.TEXT, nullable=False, transform=Transform.STRIP),
    ColumnDef("city", SqlType.TEXT, nullable=False, transform=Transform.LOWERCASE),
    ColumnDef("neighbourhood", SqlType.TEXT, transform=Transform.STRIP),
    # ── Pricing & dates ──────────────────────────────────────────────────
    ColumnDef("rent", SqlType.DOUBLE, valid_range=NumericRange(min=0, exclusive=True), hashed=True),
    ColumnDef("move_in_date", SqlType.TEXT),
    # ── Property details ─────────────────────────────────────────────────
    ColumnDef("bedrooms", SqlType.INTEGER),
    ColumnDef("bathrooms", SqlType.INTEGER),
    ColumnDef("size_sqft", SqlType.DOUBLE),
    ColumnDef("unit_type", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("agreement_type", SqlType.TEXT, transform=Transform.LOWERCASE),
    ColumnDef("furnished", SqlType.BOOLEAN, transform=Transform.TO_BOOLEAN, hashed=True),
    ColumnDef("for_rent_by", SqlType.TEXT, transform=Transform.LOWERCASE),
    # ── Location ─────────────────────────────────────────────────────────
    ColumnDef("latitude", SqlType.DOUBLE, valid_range=NumericRange(min=41.0, max=84.0)),
    ColumnDef("longitude", SqlType.DOUBLE, valid_range=NumericRange(min=-142.0, max=-52.0)),
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
    ColumnDef("pet_friendly", SqlType.TEXT, transform=Transform.LOWERCASE, hashed=True),
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
    # ── Change detection ─────────────────────────────────────────────────
    ColumnDef("row_hash", SqlType.TEXT, nullable=False, upsert=UpsertStrategy.ALWAYS_UPDATE),
    # ── Temporal tracking ────────────────────────────────────────────────
    ColumnDef("scraped_at", SqlType.TIMESTAMPTZ, nullable=False),
    ColumnDef(
        "first_seen_at",
        SqlType.TIMESTAMPTZ,
        nullable=False,
        upsert=UpsertStrategy.INSERT_ONLY,
        sql_default="NOW()",
    ),
    ColumnDef(
        "last_seen_at", SqlType.TIMESTAMPTZ, nullable=False, upsert=UpsertStrategy.ALWAYS_UPDATE
    ),
    ColumnDef(
        "loaded_at",
        SqlType.TIMESTAMPTZ,
        nullable=False,
        upsert=UpsertStrategy.MANAGED,
        sql_default="NOW()",
    ),
)


# ═══════════════════════════════════════════════════════════════════════════════
# Derived constants — neighbourhoods
# ═══════════════════════════════════════════════════════════════════════════════

# DataFrame columns for neighbourhoods (values from the Polars frame, excludes MANAGED).
NEIGHBOURHOOD_COLUMNS: list[str] = [
    c.name for c in NEIGHBOURHOOD_REGISTRY if c.upsert != UpsertStrategy.MANAGED
]

# Primary-key column names.
NEIGHBOURHOOD_PK_COLUMNS: list[str] = [c.name for c in NEIGHBOURHOOD_REGISTRY if c.is_pk]

# Columns overwritten on upsert conflict.
NEIGHBOURHOOD_UPDATE_COLUMNS: list[str] = [
    c.name for c in NEIGHBOURHOOD_REGISTRY if c.upsert == UpsertStrategy.OVERWRITE
]


# ═══════════════════════════════════════════════════════════════════════════════
# Derived constants — listings
# ═══════════════════════════════════════════════════════════════════════════════

# DataFrame columns for listings (values from the Polars frame).
# Excludes INSERT_ONLY and MANAGED — those use SQL expressions instead of %s placeholders.
LISTING_COLUMNS: list[str] = [
    c.name
    for c in LISTINGS_REGISTRY
    if c.upsert not in (UpsertStrategy.INSERT_ONLY, UpsertStrategy.MANAGED)
]

# Primary-key column names.
LISTING_PK_COLUMNS: list[str] = [c.name for c in LISTINGS_REGISTRY if c.is_pk]

# Columns included in the row hash for change detection.
HASH_COLUMNS: list[str] = [c.name for c in LISTINGS_REGISTRY if c.hashed]

# Name of the hash column.
HASH_COLUMN: str = "row_hash"

# Column tracking the most recent scrape time for a listing.
LAST_SEEN_COLUMN: str = "last_seen_at"

# Column used for temporal ordering during deduplication.
DEDUP_SORT_COLUMN: str = "scraped_at"


# ═══════════════════════════════════════════════════════════════════════════════
# Derived constants — transforms (applied to the full frame before splitting)
# ═══════════════════════════════════════════════════════════════════════════════

# Columns that need "Yes"/"No" → Boolean conversion.
BOOLEAN_COLUMNS: list[str] = [
    c.name for c in LISTINGS_REGISTRY if c.transform == Transform.TO_BOOLEAN
]

# Columns to strip whitespace only.
STRIP_COLUMNS: list[str] = [c.name for c in LISTINGS_REGISTRY if c.transform == Transform.STRIP]

# Columns to strip + lowercase (categorical / enum-like values).
LOWERCASE_COLUMNS: list[str] = [
    c.name for c in LISTINGS_REGISTRY if c.transform == Transform.LOWERCASE
]

# Columns with numeric range validation from both registries: name → NumericRange.
RANGE_VALIDATED_COLUMNS: dict[str, NumericRange] = {
    c.name: c.valid_range
    for c in (*LISTINGS_REGISTRY, *NEIGHBOURHOOD_REGISTRY)
    if c.valid_range is not None
}


# Input columns — expected from the bronze layer (derived from the scraping model).
def _build_input_columns() -> list[str]:
    """Generate the input column list from ``RentalsListing.model_fields``."""
    from real_estate_data_platform.models.listings import RentalsListing

    return list(RentalsListing.model_fields.keys())


INPUT_COLUMNS: list[str] = _build_input_columns()


# ═══════════════════════════════════════════════════════════════════════════════
# SQL generation helpers
# ═══════════════════════════════════════════════════════════════════════════════


def build_create_table_sql(
    registry: tuple[ColumnDef, ...],
    schema: str,
    table: str,
) -> str:
    """Generate a ``CREATE TABLE IF NOT EXISTS`` statement from a registry.

    Args:
        registry: Column registry to build the DDL from.
        schema: PostgreSQL schema name.
        table: Table name inside schema.

    Returns:
        SQL DDL statement.
    """
    pk_cols = [c.name for c in registry if c.is_pk]
    lines: list[str] = []
    for col in registry:
        parts = [f"    {col.name:<25s} {col.sql_type.value}"]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.sql_default:
            parts.append(f"DEFAULT {col.sql_default}")
        lines.append(" ".join(parts))

    pk_csv = ", ".join(pk_cols)
    lines.append(f"\n    PRIMARY KEY ({pk_csv})")
    body = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{body}\n)"


def build_neighbourhood_upsert_sql(schema: str, table: str) -> str:
    """Generate an ``INSERT ON CONFLICT DO UPDATE`` for the neighbourhoods table.

    Simple overwrite upsert — scores are always replaced with incoming values.
    """
    qualified = f"{schema}.{table}"
    cols_csv = ", ".join(NEIGHBOURHOOD_COLUMNS)
    placeholders = ", ".join("%s" for _ in NEIGHBOURHOOD_COLUMNS)

    # Managed columns added as SQL expressions in VALUES.
    managed = [c for c in NEIGHBOURHOOD_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
    managed_cols = ", ".join(c.name for c in managed)
    managed_vals = ", ".join(c.sql_default or "NULL" for c in managed)

    all_cols = f"{cols_csv}, {managed_cols}" if managed_cols else cols_csv
    all_vals = f"{placeholders}, {managed_vals}" if managed_vals else placeholders

    # SET clause.
    set_parts: list[str] = [f"{c} = EXCLUDED.{c}" for c in NEIGHBOURHOOD_UPDATE_COLUMNS]
    for col_def in managed:
        set_parts.append(f"{col_def.name} = {col_def.sql_default}")

    pk_csv = ", ".join(NEIGHBOURHOOD_PK_COLUMNS)
    set_clause = ",\n    ".join(set_parts)

    return (
        f"INSERT INTO {qualified} ({all_cols})\n"
        f"VALUES ({all_vals})\n"
        f"ON CONFLICT ({pk_csv}) DO UPDATE SET\n"
        f"    {set_clause}"
    )


def build_listings_upsert_sql(schema: str, table: str) -> str:
    """Generate a hash-aware ``INSERT ON CONFLICT DO UPDATE`` for the listings table.

    Behaviour:

    - **New listing** → full INSERT (``first_seen_at`` defaults to ``NOW()``).
    - **Existing, hash unchanged** → only ``last_seen_at`` is refreshed.
    - **Existing, hash changed** → all data columns are overwritten.

    A ``WHERE`` guard prevents older scrapes from regressing column values.
    """
    qualified = f"{schema}.{table}"
    cols_csv = ", ".join(LISTING_COLUMNS)
    placeholders = ", ".join("%s" for _ in LISTING_COLUMNS)

    # INSERT_ONLY + MANAGED columns use SQL expressions in VALUES.
    expr_cols = [
        c
        for c in LISTINGS_REGISTRY
        if c.upsert in (UpsertStrategy.INSERT_ONLY, UpsertStrategy.MANAGED)
    ]
    expr_cols_csv = ", ".join(c.name for c in expr_cols)
    expr_vals_csv = ", ".join(c.sql_default or "NULL" for c in expr_cols)

    all_cols = f"{cols_csv}, {expr_cols_csv}" if expr_cols_csv else cols_csv
    all_vals = f"{placeholders}, {expr_vals_csv}" if expr_vals_csv else placeholders

    # ── SET clause ───────────────────────────────────────────────────────
    set_parts: list[str] = []

    # ALWAYS_UPDATE columns — unconditional.
    always = [c.name for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.ALWAYS_UPDATE]
    set_parts.extend(f"{c} = EXCLUDED.{c}" for c in always)

    # OVERWRITE columns — conditional on hash change.
    overwrite = [c.name for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.OVERWRITE]
    set_parts.extend(
        f"{c} = CASE WHEN EXCLUDED.{HASH_COLUMN} != {qualified}.{HASH_COLUMN}"
        f" THEN EXCLUDED.{c} ELSE {qualified}.{c} END"
        for c in overwrite
    )

    # MANAGED columns — conditional on hash change.
    managed = [c for c in LISTINGS_REGISTRY if c.upsert == UpsertStrategy.MANAGED]
    set_parts.extend(
        f"{c.name} = CASE WHEN EXCLUDED.{HASH_COLUMN} != {qualified}.{HASH_COLUMN}"
        f" THEN {c.sql_default} ELSE {qualified}.{c.name} END"
        for c in managed
    )

    pk_csv = ", ".join(LISTING_PK_COLUMNS)
    set_clause = ",\n    ".join(set_parts)

    # WHERE guard: only update when incoming data is at least as recent.
    where_clause = f"WHERE EXCLUDED.{DEDUP_SORT_COLUMN} >= {qualified}.{LAST_SEEN_COLUMN}"

    return (
        f"INSERT INTO {qualified} ({all_cols})\n"
        f"VALUES ({all_vals})\n"
        f"ON CONFLICT ({pk_csv}) DO UPDATE SET\n"
        f"    {set_clause}\n"
        f"{where_clause}"
    )
