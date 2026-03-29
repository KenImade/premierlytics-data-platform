import pytest
import dagster as dg
from unittest.mock import MagicMock, patch
from io import StringIO

from premierlytics_dagster.defs.raw.assets import build_raw_asset


@pytest.fixture
def mock_context():
    context = dg.build_asset_context(
        partition_key=dg.MultiPartitionKey({"season": "2025-2026", "gameweek": "GW1"}),
    )
    return context


@pytest.fixture
def mock_minio():
    minio = MagicMock()
    minio.bucket = "test-bucket"
    return minio


class TestBuildRawAsset:
    @patch("premierlytics_dagster.defs.raw.assets.download_csv")
    @patch("premierlytics_dagster.defs.raw.assets.get_dataset_config")
    def test_downloads_and_stores(
        self, mock_config, mock_download, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/{gameweek}/matches.csv",
            is_per_gameweek=True,
        )
        mock_download.return_value = "id,name\n1,test\n"

        asset_fn = build_raw_asset("matches")
        asset_fn(mock_context, mock_minio)

        mock_download.assert_called_once_with(
            "https://example.com/2025-2026/GW1/matches.csv"
        )
        mock_minio.put_object.assert_called_once()

    @patch("premierlytics_dagster.defs.raw.assets.get_dataset_config")
    def test_skips_missing_dataset(self, mock_config, mock_context, mock_minio):
        mock_config.side_effect = ValueError("not found")

        asset_fn = build_raw_asset("fixtures")
        result = asset_fn(mock_context, mock_minio)

        assert result is None
        mock_minio.put_object.assert_not_called()

    @patch("premierlytics_dagster.defs.raw.assets.get_dataset_config")
    def test_skips_single_file_non_gw1(self, mock_config, mock_context, mock_minio):
        mock_context = dg.build_asset_context(
            partition_key=dg.MultiPartitionKey(
                {"season": "2024-2025", "gameweek": "GW5"}
            ),
        )
        mock_config.return_value = MagicMock(is_per_gameweek=False)

        asset_fn = build_raw_asset("players")
        result = asset_fn(mock_context, mock_minio)

        assert result is None
        mock_minio.put_object.assert_not_called()

    @patch("premierlytics_dagster.defs.raw.assets.download_csv")
    @patch("premierlytics_dagster.defs.raw.assets.get_dataset_config")
    def test_processes_single_file_gw1(
        self, mock_config, mock_download, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/players.csv",
            is_per_gameweek=False,
        )
        mock_download.return_value = "id,name\n1,test\n"

        asset_fn = build_raw_asset("players")
        asset_fn(mock_context, mock_minio)

        mock_minio.put_object.assert_called_once()
