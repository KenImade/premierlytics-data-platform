import dagster as dg
import io

from ..resources import MinioResource
from ..partitions import matches_partitions
from ..config import get_dataset_config

from premierlytics_dagster.helpers.clean_data import clean_csv
from premierlytics_dagster.helpers.validation import validate_csv


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_matches"],
    group_name="transformed_data",
)
def transformed_matches(context: dg.AssetExecutionContext, minio: MinioResource):
    # Get partiion keys
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "matches")

    schema = data_cfg["schema"]

    # Create raw path
    raw_data_path = f"raw/{season}/gameweek_{gameweek}/matches.csv"

    # Retrieve raw dataset
    csv_data = minio.get_object(bucket=minio.bucket, key=raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    # Clean raw dataset
    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    # Validate dataset
    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    # Save invalid data to quarantine bucket if any
    if not invalid_data.is_empty():
        quarantine_path = f"quarantine/{season}/gameweek_{gameweek}/matches.parquet"
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    # Guard against fully quarantined partitions
    if validated_data.is_empty():
        raise dg.Failure(
            description=f"All rows for {season} {gameweek} failed validation and were quarantined. Halting pipeline.",
            metadata={"quarantined_path": quarantine_path},
        )

    # Save as parquet to transformed path
    transformed_path = f"transformed/{season}/gameweek_{gameweek}/matches.parquet"
    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
            "schema": schema.__name__,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_playermatchstats"],
    group_name="transformed_data",
)
def transformed_playermatchstats(
    context: dg.AssetExecutionContext, minio: MinioResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "playermatchstats")
    schema = data_cfg["schema"]

    raw_data_path = f"raw/{season}/gameweek_{gameweek}/playermatchstats.csv"

    csv_data = minio.get_object(minio.bucket, raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    if not invalid_data.is_empty():
        quarantine_path = (
            f"quarantine/{season}/gameweek_{gameweek}/playermatchstats.parquet"
        )
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    # Guard against fully quarantined partitions
    if validated_data.is_empty():
        raise dg.Failure(
            description=f"All rows for {season} {gameweek} failed validation and were quarantined. Halting pipeline.",
            metadata={"quarantined_path": quarantine_path},
        )

    transformed_path = (
        f"transformed/{season}/gameweek_{gameweek}/playermatchstats.parquet"
    )

    buffer = io.BytesIO()

    validated_data.write_parquet(buffer)

    parquet_bytes = buffer.getvalue()

    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_players"],
    group_name="transformed_data",
)
def transformed_players(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "players")
    schema = data_cfg["schema"]

    raw_data_path = f"raw/{season}/gameweek_{gameweek}/players.csv"

    csv_data = minio.get_object(minio.bucket, raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    if not invalid_data.is_empty():
        quarantine_path = f"quarantine/{season}/gameweek_{gameweek}/players.parquet"
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    # Guard against fully quarantined partitions
    if validated_data.is_empty():
        raise dg.Failure(
            description=f"All rows for {season} {gameweek} failed validation and were quarantined. Halting pipeline.",
            metadata={"quarantined_path": quarantine_path},
        )

    transformed_path = f"transformed/{season}/gameweek_{gameweek}/players.parquet"
    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_playerstats"],
    group_name="transformed_data",
)
def transformed_playerstats(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "playerstats")
    schema = data_cfg["schema"]

    raw_data_path = f"raw/{season}/gameweek_{gameweek}/playerstats.csv"

    csv_data = minio.get_object(minio.bucket, raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    if not invalid_data.is_empty():
        quarantine_path = f"quarantine/{season}/gameweek_{gameweek}/playerstats.parquet"
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    # Guard against fully quarantined partitions
    if validated_data.is_empty():
        raise dg.Failure(
            description=f"All rows for {season} {gameweek} failed validation and were quarantined. Halting pipeline.",
            metadata={"quarantined_path": quarantine_path},
        )

    transformed_path = f"transformed/{season}/gameweek_{gameweek}/playerstats.parquet"
    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_teams"],
    group_name="transformed_data",
)
def transformed_teams(context: dg.AssetExecutionContext, minio: MinioResource):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "teams")
    schema = data_cfg["schema"]

    raw_data_path = f"raw/{season}/gameweek_{gameweek}/teams.csv"

    csv_data = minio.get_object(minio.bucket, raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    if not invalid_data.is_empty():
        quarantine_path = f"quarantine/{season}/gameweek_{gameweek}/teams.parquet"
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    transformed_path = f"transformed/{season}/gameweek_{gameweek}/teams.parquet"
    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_player_gameweek_stats"],
    group_name="transformed_data",
)
def transformed_player_gameweek_stats(
    context: dg.AssetExecutionContext, minio: MinioResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    data_cfg = get_dataset_config(season, "player_gameweek_stats")
    schema = data_cfg["schema"]

    raw_data_path = f"raw/{season}/gameweek_{gameweek}/player_gameweek_stats.csv"

    csv_data = minio.get_object(minio.bucket, raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    if not invalid_data.is_empty():
        quarantine_path = (
            f"quarantine/{season}/gameweek_{gameweek}/player_gameweek_stats.parquet"
        )
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quarantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    transformed_path = (
        f"transformed/{season}/gameweek_{gameweek}/player_gameweek_stats.parquet"
    )

    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quarantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["raw_fixtures"],
    group_name="transformed_data",
)
def transformed_fixtures(context: dg.AssetExecutionContext, minio: MinioResource):
    # Get partiion keys
    partitions: dg.MultiplePartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    AVAILABLE_SEASONS = ["2025-2026"]

    if season not in AVAILABLE_SEASONS:
        raise dg.SkipReason(f"No data available for season {season}, skipping.")

    data_cfg = get_dataset_config(season, "fixtures")
    schema = data_cfg["schema"]

    # Create raw path
    raw_data_path = f"raw/{season}/gameweek_{gameweek}/fixtures.csv"

    # Retrieve raw dataset
    csv_data = minio.get_object(bucket=minio.bucket, key=raw_data_path)

    context.log.info(f"Retrieved raw data from MinIO: {minio.bucket}/{raw_data_path}")

    # Clean raw dataset
    cleaned_data = clean_csv(csv_data, schema)
    context.log.info(f"Cleaned dataset using {schema.__name__} schema")

    # Validate dataset
    validated_data, invalid_data = validate_csv(cleaned_data, schema)

    # Save invalid data to quarantine bucket if any
    if not invalid_data.is_empty():
        quartantine_path = f"quarantine/{season}/gameweek_{gameweek}/fixtures.parquet"
        buffer = io.BytesIO()
        invalid_data.write_parquet(buffer)
        parquet_bytes = buffer.getvalue()
        minio.put_parquet(bucket=minio.bucket, key=quartantine_path, data=parquet_bytes)
        context.log.warning(
            f"{len(invalid_data)} rows failed verification and were quarantined"
        )

    # Save as parquet to transformed path
    transformed_path = f"transformed/{season}/gameweek_{gameweek}/fixtures.parquet"
    buffer = io.BytesIO()
    validated_data.write_parquet(buffer)
    parquet_bytes = buffer.getvalue()
    minio.put_parquet(bucket=minio.bucket, key=transformed_path, data=parquet_bytes)
    context.log.info(
        f"Saved cleaned dataset to MinIO: {minio.bucket}/{transformed_path}"
    )

    # Display metadata
    context.add_output_metadata(
        {
            "row_count": len(validated_data),
            "quarantined_rows": len(invalid_data),
            "quarantined_path": (
                quartantine_path if not invalid_data.is_empty() else "none"
            ),
            "path": transformed_path,
        }
    )
