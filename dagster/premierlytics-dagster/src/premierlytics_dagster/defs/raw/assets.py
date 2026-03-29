import dagster as dg
import polars as pl

from io import StringIO

from premierlytics_dagster.helpers.download_csv import (
    download_csv,
    RetryConfig,
    build_retry_policy,
)
from premierlytics_dagster.defs.resources import MinioResource
from ..partitions import matches_partitions
from ..config import get_dataset_config


def build_raw_asset(dataset_name: str):
    @dg.asset(
        name=f"raw_{dataset_name}",
        partitions_def=matches_partitions,
        group_name="raw_data",
        retry_policy=build_retry_policy(RetryConfig()),
        description=(
            f"Ingests raw Premier League {dataset_name} CSV data from the "
            f"olbauday FPL-Core-Insights GitHub repository and stores it in "
            f"MinIO at raw/{{season}}/{{gameweek}}/{dataset_name}.csv. "
            f"Skips seasons where the dataset is not configured and skips "
            f"redundant gameweeks for single-file datasets."
        ),
    )
    def _asset(context: dg.AssetExecutionContext, minio: MinioResource) -> None:
        partition: dg.MultiPartitionKey = context.partition_key  # type: ignore[assignment]
        season = partition.keys_by_dimension["season"]
        gameweek = partition.keys_by_dimension["gameweek"]

        try:
            data_cfg = get_dataset_config(season, dataset_name)
        except ValueError:
            context.log.info(
                "No %s config for season %s, skipping", dataset_name, season
            )
            return

        if not data_cfg.is_per_gameweek and gameweek != "GW1":
            context.log.info(
                "Skipping gameweek %s — %s is a single-file dataset for season %s",
                gameweek,
                dataset_name,
                season,
            )
            return

        url = data_cfg.url_template.format(season=season, gameweek=gameweek)

        context.log.info(
            "Downloading %s data for season=%s, gameweek=%s",
            dataset_name,
            season,
            gameweek,
        )

        csv_text = download_csv(url)

        key = f"raw/{season}/{gameweek}/{dataset_name}.csv"
        minio.put_object(key=key, data=csv_text)

        context.log.info("Saved to MinIO: %s/%s", minio.bucket, key)

        df = pl.read_csv(StringIO(csv_text))
        rows, columns = df.shape

        context.add_output_metadata(
            {
                "season": season,
                "gameweek": gameweek,
                "minio_path": f"{minio.bucket}/{key}",
                "row_count": dg.MetadataValue.int(rows),
                "column_count": dg.MetadataValue.int(columns),
                "columns": dg.MetadataValue.text(", ".join(df.columns)),
            }
        )

    return _asset


raw_matches = build_raw_asset("matches")
raw_playermatchstats = build_raw_asset("playermatchstats")
raw_players = build_raw_asset("players")
raw_playerstats = build_raw_asset("playerstats")
raw_teams = build_raw_asset("teams")
raw_player_gameweek_stats = build_raw_asset("player_gameweek_stats")
raw_fixtures = build_raw_asset("fixtures")
