"""Prefect tasks for bronze to silver transformations using Polars."""

import polars as pl
from prefect import get_run_logger, task
from prefect.cache_policies import NONE

from real_estate_data_platform.models.silver_schema import (
    BOOLEAN_COLUMNS,
    DEDUP_SORT_COLUMN,
    LOWERCASE_COLUMNS,
    PK_COLUMNS,
    RANGE_VALIDATED_COLUMNS,
    SILVER_COLUMNS,
    STRIP_COLUMNS,
    NumericRange,
)

_TRUTHY_VALUES = {"yes", "included"}
_FALSY_VALUES = {"no", "not included"}


def _to_boolean(col_name: str) -> pl.Expr:
    """Build a Polars expression that converts a string column to Boolean.

    Recognises common "yes"/"no" or "included"/"not included" values.
    """
    lower = pl.col(col_name).str.to_lowercase().str.strip_chars()
    return (
        pl.when(lower.is_in(_TRUTHY_VALUES))
        .then(True)
        .when(lower.is_in(_FALSY_VALUES))
        .then(False)
        .otherwise(None)
        .alias(col_name)
    )


def _apply_range(col_name: str, rng: NumericRange) -> pl.Expr:
    """Build a Polars expression that nulls values outside a valid range.

    Casts to Float64 first (strict=False) so string columns are safely
    converted — non-numeric strings become null.
    """
    col = pl.col(col_name).cast(pl.Float64, strict=False)
    if rng.min is not None and rng.max is not None:
        closed = "neither" if rng.exclusive else "both"
        cond = col.is_between(rng.min, rng.max, closed=closed)
    elif rng.min is not None:
        cond = col > rng.min if rng.exclusive else col >= rng.min
    elif rng.max is not None:
        cond = col < rng.max if rng.exclusive else col <= rng.max
    else:
        return col
    return pl.when(cond).then(col).otherwise(None).alias(col_name)


@task(cache_policy=NONE)
def transform_to_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and normalise a bronze DataFrame for the silver layer.

    Steps:
    1. Add any missing expected columns (filled with null).
    2. Drop rows where any primary-key column is null or empty.
    3. Normalise strings, convert booleans, validate numeric ranges (single pass).
    4. Deduplicate by PK columns keeping the latest record.
    5. Select only the columns needed for the silver table.

    Args:
        df: Raw Polars DataFrame read from MinIO (bronze layer)

    Returns:
        Cleaned Polars DataFrame ready for PostgreSQL upsert
    """
    logger = get_run_logger()
    initial_rows = df.height
    logger.info("Starting silver transform on %d rows", initial_rows)

    # 1 — Ensure every expected column exists (fill with null)
    missing = [pl.lit(None).alias(c) for c in SILVER_COLUMNS if c not in df.columns]
    if missing:
        df = df.with_columns(missing)

    # 2 — Drop rows where any primary-key column is null or empty
    pk_checks = [
        pl.col(pk).is_not_null() & (pl.col(pk).cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
        for pk in PK_COLUMNS
    ]
    df = df.filter(pl.all_horizontal(pk_checks))
    rows_after_pk = df.height
    dropped_pk = initial_rows - rows_after_pk
    if dropped_pk:
        logger.warning("Dropped %d rows with null/empty PK columns", dropped_pk)

    # 3 — Normalise strings, convert booleans, validate numeric ranges
    #     Batched into a single with_columns() call to avoid intermediate materialisation.
    exprs: list[pl.Expr] = []

    # strip whitespace
    exprs.extend(pl.col(c).str.strip_chars().alias(c) for c in STRIP_COLUMNS if c in df.columns)

    # strip + lowercase (categorical values)
    exprs.extend(
        pl.col(c).str.strip_chars().str.to_lowercase().alias(c)
        for c in LOWERCASE_COLUMNS
        if c in df.columns
    )

    # "Yes"/"Included" → True, "No"/"Not Included" → False
    exprs.extend(_to_boolean(col) for col in BOOLEAN_COLUMNS if col in df.columns)

    # null out values outside valid ranges
    exprs.extend(_apply_range(col_name, rng) for col_name, rng in RANGE_VALIDATED_COLUMNS.items())

    if exprs:
        df = df.with_columns(exprs)

    # 4 — Deduplicate: keep the most recent record for each PK
    rows_before_dedup = df.height
    df = df.sort(DEDUP_SORT_COLUMN, descending=True).unique(subset=PK_COLUMNS, keep="first")
    dropped_dedup = rows_before_dedup - df.height
    if dropped_dedup:
        logger.info("Dedup removed %d duplicate rows", dropped_dedup)

    # 5 — Select silver columns in the expected order
    df = df.select(SILVER_COLUMNS)

    logger.info(
        "Silver transform complete: %d to %d rows (-%d PK invalid, -%d duplicates)",
        initial_rows,
        df.height,
        dropped_pk,
        dropped_dedup,
    )
    return df
