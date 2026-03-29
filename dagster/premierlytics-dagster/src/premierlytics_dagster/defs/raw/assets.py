import dagster as dg
import json
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


@dg.asset(
    partitions_def=matches_partitions,
    group_name="raw_data",
    retry_policy=build_retry_policy(RetryConfig()),
)
def raw_matches(context: dg.AssetExecutionContext, minio: MinioResource) -> None:
    """
    Ingests raw Premier League matches CSV data from the olbauday
    FPL-Core-Insights GitHub repository and stores it in MinIO.

    Partitioned by season and gameweek. Output lands at:
        s3://<bucket>/raw/{season}/{gameweek}/matches.csv

    Downstream assets should read directly from MinIO rather than
    depending on this asset's return value.
    """

    # Get gameweek and season
    partition: dg.MultiPartitionKey = context.partition_key  # type: ignore[assignment]
    season = partition.keys_by_dimension["season"]
    gameweek = partition.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "matches")

    url_template = data_cfg.get("url_template")

    if not url_template:
        raise ValueError(
            f"Missing 'url_template' in dataset config for season={season}, "
            f"dataset='matches'"
        )

    url = url_template.format(season=season, gameweek=gameweek)

    context.log.info(
        "Downloading matches data for season=%s, gameweek=%s", season, gameweek
    )
    csv_text = download_csv(url)

    key = f"raw/{season}/{gameweek}/matches.csv"
    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info("Saved to MinIO: %s/%s", minio.bucket, key)

    df = pl.read_csv(StringIO(csv_text))
    rows, columns = df.shape
    columns_list = df.columns

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "row_count": dg.MetadataValue.int(rows),
            "column_count": dg.MetadataValue.int(columns),
            "columns": dg.MetadataValue.text(", ".join(columns_list)),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_playermatchstats(context: dg.AssetExecutionContext, minio: MinioResource):
    """Raw PlayerMatchStats CSV files"""
    partition: dg.MultiPartitionKey = context.partition_key  # type: ignore[assignment]
    season = partition.keys_by_dimension["season"]
    gameweek = partition.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "playermatchstats")

    url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(
        f"Downloading playermatchstats data for season={season}, gameweek={gameweek}"
    )

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/playermatchstats.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_players(context: dg.AssetExecutionContext, minio: MinioResource):
    """Raw Players CSV data"""
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "players")

    if season == "2024-2025":
        url = data_cfg["url_template"].format(season=season)
    else:
        url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(
        f"Downloading players data for season={season}, gameweek={gameweek}"
    )

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/players.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_playerstats(context: dg.AssetExecutionContext, minio: MinioResource):
    """Raw PlayerStats CSV data"""
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "playerstats")

    if season == "2024-2025":
        url = data_cfg["url_template"].format(season=season)
    else:
        url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(
        f"Donwloading playerstats data for season={season}, gameweek={gameweek}"
    )

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/playerstats.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_teams(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "teams")

    if season == "2024-2025":
        url = data_cfg["url_template"].format(season=season)
    else:
        url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(f"Donwloading teams data for season={season}, gameweek={gameweek}")

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/teams.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_player_gameweek_stats(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "player_gameweek_stats")

    url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(
        f"Donwloading player_gameweek_stats data for season={season}, gameweek={gameweek}"
    )

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/player_gameweek_stats.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )


@dg.asset(partitions_def=matches_partitions, group_name="raw_data")
def raw_fixtures(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "fixtures")

    AVAILABLE_SEASONS = ["2025-2026"]

    if season not in AVAILABLE_SEASONS:
        raise dg.SkipReason(f"No data available for season {season}, skipping.")

    url = data_cfg["url_template"].format(season=season, gameweek=gameweek)

    context.log.info(
        f"Donwloading fixtures data for season={season}, gameweek={gameweek}"
    )

    csv_text = download_csv(url)

    report = data_report(csv_text)

    key = f"raw/{season}/gameweek_{gameweek}/fixtures.csv"

    minio.put_object(bucket=minio.bucket, key=key, data=csv_text)

    context.log.info(f"Saved to MinIO: {minio.bucket}/{key}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "minio_path": f"{minio.bucket}/{key}",
            "dataset_report": dg.MetadataValue.json(
                json.loads(json.dumps(report, default=str))
            ),
        }
    )
