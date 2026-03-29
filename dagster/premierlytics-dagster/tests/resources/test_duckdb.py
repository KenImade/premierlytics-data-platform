import pytest
from pathlib import Path

from premierlytics_dagster.defs.resources.duckdb import DuckDBResource


@pytest.fixture
def duckdb_resource(tmp_path: Path) -> DuckDBResource:
    db_path = str(tmp_path / "test.duckdb")
    return DuckDBResource(db_path=db_path)


def test_connection_creates_directory(tmp_path: Path):
    db_path = str(tmp_path / "nested" / "dir" / "test.duckdb")
    resource = DuckDBResource(db_path=db_path)
    with resource.connection() as conn:
        result = conn.execute("SELECT 1").fetchone()
    assert result == (1,)
    assert Path(db_path).parent.exists()


def test_read_only_connection(duckdb_resource: DuckDBResource):
    # Create the database first
    with duckdb_resource.connection() as conn:
        conn.execute("CREATE TABLE test (id INT)")
        conn.execute("INSERT INTO test VALUES (1), (2)")

    with duckdb_resource.read_only_connection() as conn:
        rows = conn.execute("SELECT * FROM test").fetchall()
    assert len(rows) == 2


def test_read_only_rejects_writes(duckdb_resource: DuckDBResource):
    with duckdb_resource.connection() as conn:
        conn.execute("CREATE TABLE test (id INT)")

    with pytest.raises(Exception):
        with duckdb_resource.read_only_connection() as conn:
            conn.execute("INSERT INTO test VALUES (1)")
