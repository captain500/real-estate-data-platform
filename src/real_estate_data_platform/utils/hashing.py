"""Utility for computing row-level change-detection hashes."""

from __future__ import annotations

import hashlib

import polars as pl


def build_row_hash_expr(columns: list[str]) -> pl.Expr:
    """Build a Polars expression that computes an MD5 hash over the given columns.

    Null values are coerced to empty strings before hashing so that the
    result is deterministic regardless of null positions.

    Args:
        columns: Column names to include in the hash computation.

    Returns:
        Polars expression producing a hex-encoded MD5 string.
    """
    concat = pl.concat_str(
        [pl.col(c).cast(pl.Utf8).fill_null("") for c in columns],
        separator="|",
    )
    return concat.map_batches(
        lambda s: pl.Series(hashlib.md5(v.encode()).hexdigest() for v in s),
        return_dtype=pl.Utf8,
    )
