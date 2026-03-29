"""Tests for real_estate_data_platform.utils.hashing."""

import polars as pl

from real_estate_data_platform.utils.hashing import build_row_hash_expr


class TestBuildRowHashExpr:
    """Tests for build_row_hash_expr — row-level MD5 change detection."""

    def test_deterministic_hash(self):
        """Same input always produces the same hash."""
        df = pl.DataFrame({"a": ["hello"], "b": ["world"]})
        h1 = df.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        h2 = df.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        assert h1 == h2

    def test_different_values_produce_different_hashes(self):
        df = pl.DataFrame({"a": ["x", "y"], "b": ["1", "1"]})
        hashes = df.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"].to_list()
        assert hashes[0] != hashes[1]

    def test_column_order_matters(self):
        """Hash(a|b) != Hash(b|a) — column ordering is significant."""
        df = pl.DataFrame({"a": ["x"], "b": ["y"]})
        h_ab = df.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        h_ba = df.select(build_row_hash_expr(["b", "a"]).alias("h"))["h"][0]
        assert h_ab != h_ba

    def test_null_handling(self):
        """Nulls are coerced to empty string — should not crash and should be deterministic."""
        df = pl.DataFrame({"a": [None], "b": ["val"]})
        result = df.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hex length

    def test_null_vs_empty_string_distinction(self):
        """Null and empty string produce the same hash (since null → '')."""
        df_null = pl.DataFrame({"a": [None], "b": ["x"]})
        df_empty = pl.DataFrame({"a": [""], "b": ["x"]})
        h_null = df_null.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        h_empty = df_empty.select(build_row_hash_expr(["a", "b"]).alias("h"))["h"][0]
        # By design, null and "" are treated the same
        assert h_null == h_empty

    def test_hash_is_valid_md5_hex(self):
        df = pl.DataFrame({"a": ["test"]})
        h = df.select(build_row_hash_expr(["a"]).alias("h"))["h"][0]
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_separator_prevents_collision(self):
        """'ab' + 'c' should hash differently from 'a' + 'bc' thanks to '|' separator."""
        df1 = pl.DataFrame({"x": ["ab"], "y": ["c"]})
        df2 = pl.DataFrame({"x": ["a"], "y": ["bc"]})
        h1 = df1.select(build_row_hash_expr(["x", "y"]).alias("h"))["h"][0]
        h2 = df2.select(build_row_hash_expr(["x", "y"]).alias("h"))["h"][0]
        assert h1 != h2
