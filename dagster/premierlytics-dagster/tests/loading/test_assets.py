import pytest
import dagster as dg
import polars as pl
from unittest.mock import MagicMock, patch
import io

from premierlytics_dagster.defs.loading.assets import build_loaded_asset


@pytest.fixture
def mock_context():
    return dg.build_asset_context(
        partition_key=dg.MultiPartitionKey({"season": "2025-2026", "gameweek": "GW1"}),
    )


@pytest.fixture
def mock_minio():
    df = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()

    minio = MagicMock()
    minio.bucket = "test-bucket"
    minio.get_bytes.return_value = parquet_bytes
    return minio


@pytest.fixture
def mock_duckdb():
    conn = MagicMock()
    duckdb = MagicMock()
    duckdb.connection.return_value.__enter__ = MagicMock(return_value=conn)
    duckdb.connection.return_value.__exit__ = MagicMock(return_value=False)
    duckdb.db_path = "/tmp/test.duckdb"
    return duckdb


class TestBuildLoadedAsset:
    @patch(
        "premierlytics_dagster.defs.loading.assets.load_sql",
        return_value="CREATE TABLE IF NOT EXISTS test",
    )
    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_loads_data(
        self, mock_config, mock_sql, mock_context, mock_minio, mock_duckdb
    ):
        mock_config.return_value = MagicMock(
            is_per_gameweek=True,
            add_gameweek_column=False,
            rename_columns={},
        )

        asset_fn = build_loaded_asset("matches")
        asset_fn(mock_context, mock_minio, mock_duckdb)

        mock_minio.get_bytes.assert_called_once()
        conn = mock_duckdb.connection.return_value.__enter__.return_value
        assert conn.execute.call_count == 3  # CREATE, DELETE, INSERT

    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_skips_missing_dataset(
        self, mock_config, mock_context, mock_minio, mock_duckdb
    ):
        mock_config.side_effect = ValueError("not found")

        asset_fn = build_loaded_asset("fixtures")
        result = asset_fn(mock_context, mock_minio, mock_duckdb)

        assert result is None
        mock_minio.get_bytes.assert_not_called()

    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_skips_single_file_non_gw1(self, mock_config, mock_minio, mock_duckdb):
        context = dg.build_asset_context(
            partition_key=dg.MultiPartitionKey(
                {"season": "2024-2025", "gameweek": "GW5"}
            ),
        )
        mock_config.return_value = MagicMock(is_per_gameweek=False)

        asset_fn = build_loaded_asset("players")
        result = asset_fn(context, mock_minio, mock_duckdb)

        assert result is None
        mock_minio.get_bytes.assert_not_called()

    @patch(
        "premierlytics_dagster.defs.loading.assets.load_sql",
        return_value="CREATE TABLE IF NOT EXISTS test",
    )
    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_adds_gameweek_column(
        self, mock_config, mock_sql, mock_context, mock_minio, mock_duckdb
    ):
        mock_config.return_value = MagicMock(
            is_per_gameweek=True,
            add_gameweek_column=True,
            rename_columns={},
        )

        asset_fn = build_loaded_asset("playermatchstats")
        asset_fn(mock_context, mock_minio, mock_duckdb)

        conn = mock_duckdb.connection.return_value.__enter__.return_value
        insert_call = conn.execute.call_args_list[2]
        insert_sql = insert_call[0][0]
        assert "gameweek" in insert_sql

    @patch(
        "premierlytics_dagster.defs.loading.assets.load_sql",
        return_value="CREATE TABLE IF NOT EXISTS test",
    )
    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_renames_columns(self, mock_config, mock_sql, mock_minio, mock_duckdb):
        df = pl.DataFrame({"id": [1], "gw": [5]})
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        mock_minio.get_bytes.return_value = buffer.getvalue()

        context = dg.build_asset_context(
            partition_key=dg.MultiPartitionKey(
                {"season": "2025-2026", "gameweek": "GW5"}
            ),
        )
        mock_config.return_value = MagicMock(
            is_per_gameweek=True,
            add_gameweek_column=False,
            rename_columns={"gw": "gameweek"},
        )

        asset_fn = build_loaded_asset("playerstats")
        asset_fn(context, mock_minio, mock_duckdb)

        conn = mock_duckdb.connection.return_value.__enter__.return_value
        insert_call = conn.execute.call_args_list[2]
        insert_sql = insert_call[0][0]
        assert "gameweek" in insert_sql
        assert "gw" not in insert_sql

    @patch(
        "premierlytics_dagster.defs.loading.assets.load_sql",
        return_value="CREATE TABLE IF NOT EXISTS test",
    )
    @patch("premierlytics_dagster.defs.loading.assets.get_dataset_config")
    def test_delete_uses_correct_gameweek_number(
        self, mock_config, mock_sql, mock_context, mock_minio, mock_duckdb
    ):
        mock_config.return_value = MagicMock(
            is_per_gameweek=True,
            add_gameweek_column=False,
            rename_columns={},
        )

        asset_fn = build_loaded_asset("matches")
        asset_fn(mock_context, mock_minio, mock_duckdb)

        conn = mock_duckdb.connection.return_value.__enter__.return_value
        delete_call = conn.execute.call_args_list[1]
        params = delete_call[0][1]
        assert params == ["2025-2026", 1]
