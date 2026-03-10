"""Generic PostgreSQL storage backend.

This connector is schema-agnostic: it receives pre-built SQL statements and
column lists from the caller so it can be reused across different layers or
tables without importing domain-specific modules.
"""

import logging
from typing import Self

import polars as pl
import psycopg
import psycopg.sql

logger = logging.getLogger(__name__)


class PostgresStorage:
    """Generic PostgreSQL storage backend.

    The caller injects all table-specific details (SQL, columns) so this class
    stays decoupled from any particular schema definition.
    """

    _VERIFY_SQL = (
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
    )

    def __init__(
        self,
        dsn: str,
        schema: str,
        table: str,
        upsert_sql: str,
        columns: list[str],
        create_table_sql: str | None = None,
        auto_create_schema: bool = False,
    ) -> None:
        """Initialize PostgreSQL connection and prepare the schema.

        Args:
            dsn: PostgreSQL DSN connection string
            schema: PostgreSQL schema name
            table: Table name inside schema
            upsert_sql: Pre-built INSERT … ON CONFLICT SQL with %s placeholders
            columns: Column names to select from the DataFrame before upserting
            create_table_sql: Pre-built CREATE TABLE SQL (required when auto_create_schema=True)
            auto_create_schema: If True, will create the schema/table if they don't exist
        """
        self._schema = schema
        self._table = table
        self._qualified = f"{schema}.{table}"
        self._upsert_sql = upsert_sql
        self._columns = columns
        self._create_table_sql = create_table_sql

        self.conn = psycopg.connect(dsn, autocommit=False)
        try:
            if auto_create_schema:
                if not create_table_sql:
                    raise ValueError("create_table_sql is required when auto_create_schema=True")
                self._create_schema()
            else:
                self._verify_schema()
        except Exception:
            self.conn.close()
            raise

    def _create_schema(self) -> None:
        """Create the schema and table (DEV only)."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {psycopg.sql.Identifier(self._schema).as_string(self.conn)}"
            )
            cur.execute(self._create_table_sql)
        self.conn.commit()
        logger.info("%s created (auto_create_schema=True)", self._qualified)

    def _verify_schema(self) -> None:
        """Verify the schema and table exist, raise if missing."""
        with self.conn.cursor() as cur:
            cur.execute(self._VERIFY_SQL, (self._schema, self._table))
            if cur.fetchone() is None:
                raise RuntimeError(
                    f"Table {self._qualified} does not exist. "
                    "Run migrations or set auto_create_schema=True for development."
                )
        self.conn.commit()
        logger.info("%s verified (auto_create_schema=False)", self._qualified)

    def upsert(self, df: pl.DataFrame) -> int:
        """Upsert rows from a Polars DataFrame into the target table.

        Args:
            df: Polars DataFrame with columns matching ``self._columns``

        Returns:
            Total number of rows upserted

        Raises:
            psycopg.OperationalError: If the database connection is dead
        """
        rows = df.select(self._columns).rows()
        total = len(rows)
        if total == 0:
            logger.info("No records to upsert into %s", self._qualified)
            return 0

        try:
            with self.conn.cursor() as cur:
                cur.executemany(self._upsert_sql, rows)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        logger.info("Upserted %d rows into %s", total, self._qualified)
        return total

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
