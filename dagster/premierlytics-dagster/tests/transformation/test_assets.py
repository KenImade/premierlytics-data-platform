import pytest
import dagster as dg
from unittest.mock import MagicMock, patch
import polars as pl

from premierlytics_dagster.defs.transformation.assets import build_transformed_asset


@pytest.fixture
def mock_context():
    return dg.build_asset_context(
        partition_key=dg.MultiPartitionKey({"season": "2025-2026", "gameweek": "GW1"}),
    )


@pytest.fixture
def mock_minio():
    minio = MagicMock()
    minio.bucket = "test-bucket"
    minio.get_object.return_value = "id,name,team\n1,alice,arsenal\n2,bob,chelsea\n"
    return minio


class TestBuildTransformedAsset:
    @patch("premierlytics_dagster.defs.transformation.assets.validate_required_fields")
    @patch("premierlytics_dagster.defs.transformation.assets.clean_csv")
    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_transforms_and_stores(
        self, mock_config, mock_clean, mock_validate, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/{gameweek}/matches.csv",
            is_per_gameweek=True,
            validation_schema=MagicMock,
        )
        cleaned_df = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        mock_clean.return_value = cleaned_df
        mock_validate.return_value = (cleaned_df, pl.DataFrame())

        asset_fn = build_transformed_asset("matches")
        asset_fn(mock_context, mock_minio)

        mock_minio.get_object.assert_called_once()
        mock_clean.assert_called_once()
        mock_validate.assert_called_once()
        assert mock_minio.put_object.call_count == 1

    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_skips_missing_dataset(self, mock_config, mock_context, mock_minio):
        mock_config.side_effect = ValueError("not found")

        asset_fn = build_transformed_asset("fixtures")
        result = asset_fn(mock_context, mock_minio)

        assert result is None
        mock_minio.get_object.assert_not_called()

    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_skips_single_file_non_gw1(self, mock_config, mock_minio):
        context = dg.build_asset_context(
            partition_key=dg.MultiPartitionKey(
                {"season": "2024-2025", "gameweek": "GW5"}
            ),
        )
        mock_config.return_value = MagicMock(is_per_gameweek=False)

        asset_fn = build_transformed_asset("players")
        result = asset_fn(context, mock_minio)

        assert result is None
        mock_minio.get_object.assert_not_called()

    @patch("premierlytics_dagster.defs.transformation.assets.validate_required_fields")
    @patch("premierlytics_dagster.defs.transformation.assets.clean_csv")
    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_quarantines_invalid_rows(
        self, mock_config, mock_clean, mock_validate, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/{gameweek}/matches.csv",
            is_per_gameweek=True,
            validation_schema=MagicMock,
        )
        valid_df = pl.DataFrame({"id": [1], "name": ["alice"]})
        invalid_df = pl.DataFrame(
            {"id": [2], "name": [None], "_validation_error": ["name is null"]}
        )
        mock_clean.return_value = pl.DataFrame({"id": [1, 2], "name": ["alice", None]})
        mock_validate.return_value = (valid_df, invalid_df)

        asset_fn = build_transformed_asset("matches")
        asset_fn(mock_context, mock_minio)

        # Two put_object calls: one for quarantine, one for transformed
        assert mock_minio.put_object.call_count == 2

    @patch("premierlytics_dagster.defs.transformation.assets.validate_required_fields")
    @patch("premierlytics_dagster.defs.transformation.assets.clean_csv")
    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_no_quarantine_when_all_valid(
        self, mock_config, mock_clean, mock_validate, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/{gameweek}/matches.csv",
            is_per_gameweek=True,
            validation_schema=MagicMock,
        )
        cleaned_df = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        mock_clean.return_value = cleaned_df
        mock_validate.return_value = (cleaned_df, pl.DataFrame())

        asset_fn = build_transformed_asset("matches")
        asset_fn(mock_context, mock_minio)

        # Only one put_object call: transformed data, no quarantine
        assert mock_minio.put_object.call_count == 1

    @patch("premierlytics_dagster.defs.transformation.assets.validate_required_fields")
    @patch("premierlytics_dagster.defs.transformation.assets.clean_csv")
    @patch("premierlytics_dagster.defs.transformation.assets.get_dataset_config")
    def test_stores_as_parquet(
        self, mock_config, mock_clean, mock_validate, mock_context, mock_minio
    ):
        mock_config.return_value = MagicMock(
            url_template="https://example.com/{season}/{gameweek}/matches.csv",
            is_per_gameweek=True,
            validation_schema=MagicMock,
        )
        cleaned_df = pl.DataFrame({"id": [1], "name": ["alice"]})
        mock_clean.return_value = cleaned_df
        mock_validate.return_value = (cleaned_df, pl.DataFrame())

        asset_fn = build_transformed_asset("matches")
        asset_fn(mock_context, mock_minio)

        call_kwargs = mock_minio.put_object.call_args
        assert "transformed/" in call_kwargs.kwargs.get(
            "key", call_kwargs[1].get("key", "")
        )
