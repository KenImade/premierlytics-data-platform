import dagster as dg
import duckdb
from contextlib import contextmanager
from collections.abc import Generator
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DuckDBResource(dg.ConfigurableResource):
    """
    Dagster resource wrapping a DuckDB connection.

    Note: DuckDB enforces a single-writer constraint. Only one connection
    can write at a time. Use Dagster's op_tags with a concurrency key
    to prevent concurrent write operations.

    Read-only connections bypass this constraint and can run concurrently.
    """

    db_path: str = dg.EnvVar("DUCKDB_PATH")  # "/data/duckdb/premierlytics.duckdb"

    def _ensure_directory(self) -> None:
        parent = Path(self.db_path).parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
            logger.info("Created directory: %s", parent)

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Yields a read-write DuckDB connection."""
        self._ensure_directory()
        conn = duckdb.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def read_only_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Yields a read-only DuckDB connection.

        Safe to use concurrently — not subject to the single-writer constraint.
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            yield conn
        finally:
            conn.close()
