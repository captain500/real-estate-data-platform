"""Unit tests for PostgresStorage connector."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from real_estate_data_platform.connectors.postgres import PostgresStorage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_DSN = "postgresql://user:pass@localhost:5432/testdb"
_SCHEMA = "silver"
_TABLE = "rental_listings"
_UPSERT_SQL = "INSERT INTO silver.rental_listings (a, b) VALUES (%s, %s) ON CONFLICT DO NOTHING"
_COLUMNS = ["a", "b"]
_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS silver.rental_listings (a TEXT, b TEXT)"


def _build_storage(
    *,
    auto_create_schema: bool = False,
    create_table_sql: str | None = None,
    verify_exists: bool = True,
) -> PostgresStorage:
    """Build a PostgresStorage with a mocked psycopg.connect.

    When ``verify_exists=True`` (default) the mocked cursor makes
    ``_verify_schema`` succeed by returning a row.
    """
    with patch("real_estate_data_platform.connectors.postgres.psycopg") as mock_psycopg:
        mock_conn = MagicMock()
        mock_psycopg.connect.return_value = mock_conn

        # Make cursor context-manager work
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        if verify_exists:
            mock_cursor.fetchone.return_value = (1,)

        # psycopg.sql.Identifier needs to work for _create_schema
        mock_identifier = MagicMock()
        mock_identifier.as_string.return_value = '"silver"'
        mock_psycopg.sql.Identifier.return_value = mock_identifier

        storage = PostgresStorage(
            dsn=_DSN,
            schema=_SCHEMA,
            table=_TABLE,
            upsert_sql=_UPSERT_SQL,
            columns=_COLUMNS,
            create_table_sql=create_table_sql,
            auto_create_schema=auto_create_schema,
        )
    return storage


# ---------------------------------------------------------------------------
# __init__ — verify mode (default)
# ---------------------------------------------------------------------------
class TestInit:
    """Tests for PostgresStorage initialization."""

    def test_stores_attributes(self):
        storage = _build_storage()
        assert storage._schema == _SCHEMA
        assert storage._table == _TABLE
        assert storage._qualified == f"{_SCHEMA}.{_TABLE}"
        assert storage._upsert_sql == _UPSERT_SQL
        assert storage._columns == _COLUMNS

    def test_verify_mode_queries_information_schema(self):
        with patch("real_estate_data_platform.connectors.postgres.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_psycopg.connect.return_value = mock_conn

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (1,)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            PostgresStorage(
                dsn=_DSN,
                schema=_SCHEMA,
                table=_TABLE,
                upsert_sql=_UPSERT_SQL,
                columns=_COLUMNS,
            )

            mock_cursor.execute.assert_called_once_with(
                PostgresStorage._VERIFY_SQL, (_SCHEMA, _TABLE)
            )
            mock_conn.commit.assert_called_once()

    def test_verify_mode_raises_when_table_missing(self):
        with patch("real_estate_data_platform.connectors.postgres.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_psycopg.connect.return_value = mock_conn

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = None
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            with pytest.raises(RuntimeError, match="does not exist"):
                PostgresStorage(
                    dsn=_DSN,
                    schema=_SCHEMA,
                    table=_TABLE,
                    upsert_sql=_UPSERT_SQL,
                    columns=_COLUMNS,
                )
            mock_conn.close.assert_called_once()

    def test_auto_create_requires_create_table_sql(self):
        with patch("real_estate_data_platform.connectors.postgres.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_psycopg.connect.return_value = mock_conn

            with pytest.raises(ValueError, match="create_table_sql is required"):
                PostgresStorage(
                    dsn=_DSN,
                    schema=_SCHEMA,
                    table=_TABLE,
                    upsert_sql=_UPSERT_SQL,
                    columns=_COLUMNS,
                    auto_create_schema=True,
                    create_table_sql=None,
                )
            mock_conn.close.assert_called_once()

    def test_auto_create_executes_create_schema_and_table(self):
        with patch("real_estate_data_platform.connectors.postgres.psycopg") as mock_psycopg:
            mock_conn = MagicMock()
            mock_psycopg.connect.return_value = mock_conn

            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            mock_identifier = MagicMock()
            mock_identifier.as_string.return_value = '"silver"'
            mock_psycopg.sql.Identifier.return_value = mock_identifier

            PostgresStorage(
                dsn=_DSN,
                schema=_SCHEMA,
                table=_TABLE,
                upsert_sql=_UPSERT_SQL,
                columns=_COLUMNS,
                create_table_sql=_CREATE_TABLE_SQL,
                auto_create_schema=True,
            )

            calls = mock_cursor.execute.call_args_list
            assert len(calls) == 2
            assert 'CREATE SCHEMA IF NOT EXISTS "silver"' in calls[0].args[0]
            assert calls[1].args[0] == _CREATE_TABLE_SQL
            mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------
class TestUpsert:
    """Tests for the upsert method."""

    def test_upserts_all_rows(self):
        storage = _build_storage()
        df = pl.DataFrame({"a": ["x", "y"], "b": ["1", "2"]})

        result = storage.upsert(df)

        assert result == 2
        storage.conn.cursor.return_value.__enter__.return_value.executemany.assert_called_once_with(
            _UPSERT_SQL, [("x", "1"), ("y", "2")]
        )
        storage.conn.commit.assert_called()

    def test_returns_zero_for_empty_dataframe(self):
        storage = _build_storage()
        df = pl.DataFrame({"a": [], "b": []})

        result = storage.upsert(df)

        assert result == 0
        storage.conn.cursor.return_value.__enter__.return_value.executemany.assert_not_called()

    def test_selects_only_configured_columns(self):
        storage = _build_storage()
        df = pl.DataFrame({"a": ["x"], "b": ["1"], "extra": ["ignored"]})

        result = storage.upsert(df)

        assert result == 1
        storage.conn.cursor.return_value.__enter__.return_value.executemany.assert_called_once_with(
            _UPSERT_SQL, [("x", "1")]
        )

    def test_rollback_on_execute_error(self):
        storage = _build_storage()
        df = pl.DataFrame({"a": ["x"], "b": ["1"]})

        storage.conn.cursor.return_value.__enter__.return_value.executemany.side_effect = (
            RuntimeError("DB error")
        )

        with pytest.raises(RuntimeError, match="DB error"):
            storage.upsert(df)

        storage.conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# close / context manager
# ---------------------------------------------------------------------------
class TestCloseAndContextManager:
    """Tests for close() and __enter__/__exit__."""

    def test_close_closes_connection(self):
        storage = _build_storage()
        storage.close()
        storage.conn.close.assert_called_once()

    def test_enter_returns_self(self):
        storage = _build_storage()
        assert storage.__enter__() is storage

    def test_exit_closes_connection(self):
        storage = _build_storage()
        storage.__exit__(None, None, None)
        storage.conn.close.assert_called_once()

    def test_context_manager_closes_on_exit(self):
        storage = _build_storage()
        with storage as s:
            assert s is storage
        storage.conn.close.assert_called_once()
