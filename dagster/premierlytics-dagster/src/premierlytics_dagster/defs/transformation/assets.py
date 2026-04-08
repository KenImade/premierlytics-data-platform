import dagster as dg
import io
import polars as pl

from ..resources import S3Resource
from ..partitions import matches_partitions
from ..config import get_dataset_config

from premierlytics_dagster.helpers.clean_data import clean_csv
from premierlytics_dagster.helpers.validation import validate_required_fields


def build_transformed_asset(dataset_name: str):

    def _write_parquet_to_s3(s3: S3Resource, key: str, df: pl.DataFrame):
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        s3.put_object(
            key=key, data=buffer.getvalue(), content_type="application/octet-stream"
        )

    @dg.asset(
        name=f"transformed_{dataset_name}",
        partitions_def=matches_partitions,
        deps=[f"raw_{dataset_name}"],
        group_name="transformed_data",
        description=(
            f"Retrieves raw data from raw/{{season}}/{{gameweek}}/{dataset_name}.csv. "
            f"Transforms the raw data applying data cleaning and schema validation steps "
            f"The transformed data is stored as parquet in S3 at "
            f"transformed/{{season}}/{{gameweek}}/{dataset_name}.parquet "
            f"Skips redundant gameweeks for single-file datasets."
        ),
    )
    def _asset(context: dg.AssetExecutionContext, s3: S3Resource) -> None:
        partitions: dg.MultiPartitionKey = context.partition_key  # type: ignore[assignment]
        season = partitions.keys_by_dimension["season"]
        gameweek = partitions.keys_by_dimension["gameweek"]

        try:
            data_cfg = get_dataset_config(season, dataset_name)
        except ValueError:
            context.log.info(
                "No %s config for season %s, skipping", dataset_name, season
            )
            return

        if not data_cfg.is_per_gameweek and gameweek != "GW1":
            context.log.info(
                "Skipping gameweek %s - %s is a single-file dataset for season %s",
                gameweek,
                dataset_name,
                season,
            )
            return

        raw_data_path = f"raw/{season}/{gameweek}/{dataset_name}.csv"
        dataset_schema = data_cfg.validation_schema

        csv_data = s3.get_object(key=raw_data_path)

        context.log.info(
            "Retrieved raw data from S3: s3://%s/%s", s3.bucket, raw_data_path
        )

        cleaned_data = clean_csv(csv_data, dataset_schema)

        validated_data, invalid_data = validate_required_fields(
            cleaned_data, dataset_schema
        )

        quarantine_path = f"quarantine/{season}/{gameweek}/{dataset_name}.parquet"

        if not invalid_data.is_empty():
            _write_parquet_to_s3(s3=s3, key=quarantine_path, df=invalid_data)
            context.log.warning(
                "%d rows failed verification and were quarantined", len(invalid_data)
            )

        transformed_path = f"transformed/{season}/{gameweek}/{dataset_name}.parquet"

        _write_parquet_to_s3(s3=s3, key=transformed_path, df=validated_data)
        context.log.info(
            "Saved cleaned dataset to S3: s3://%s/%s", s3.bucket, transformed_path
        )

        # Display metadata
        context.add_output_metadata(
            {
                "season": season,
                "gameweek": gameweek,
                "s3_path": f"s3://{s3.bucket}/{transformed_path}",
                "row_count": dg.MetadataValue.int(len(validated_data)),
                "quarantined_rows": dg.MetadataValue.int(len(invalid_data)),
                "quarantined_path": (
                    quarantine_path if not invalid_data.is_empty() else "none"
                ),
            }
        )

    return _asset


transformed_matches = build_transformed_asset("matches")
transformed_playermatchstats = build_transformed_asset("playermatchstats")
transformed_players = build_transformed_asset("players")
transformed_playerstats = build_transformed_asset("playerstats")
transformed_teams = build_transformed_asset("teams")
transformed_player_gameweek_stats = build_transformed_asset("player_gameweek_stats")
transformed_fixtures = build_transformed_asset("fixtures")
