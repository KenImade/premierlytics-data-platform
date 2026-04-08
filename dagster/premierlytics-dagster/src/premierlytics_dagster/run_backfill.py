"""
Entrypoint for running a full historical backfill.

Iterates over all configured seasons and gameweeks, running the pipeline
for each partition in order. Failures are logged and skipped so the backfill
continues to completion.

Usage:
    # Backfill all configured seasons and gameweeks
    python -m premierlytics_dagster.run_backfill

    # Backfill a specific season only
    python -m premierlytics_dagster.run_backfill --season 2024-2025

    # Backfill up to a specific gameweek (e.g. mid-season)
    python -m premierlytics_dagster.run_backfill --season 2025-2026 --max-gw 32
"""

import sys
import argparse
import dagster as dg

from premierlytics_dagster.definitions import defs
from premierlytics_dagster.defs.config import SEASON_CONFIG


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a historical backfill")
    parser.add_argument(
        "--season",
        help="Season to backfill (e.g. 2024-2025). Defaults to all configured seasons.",
    )
    parser.add_argument(
        "--max-gw",
        type=int,
        default=38,
        help="Last gameweek to include (default: 38)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    seasons = [args.season] if args.season else list(SEASON_CONFIG.keys())
    gameweeks = [f"GW{gw}" for gw in range(1, args.max_gw + 1)]

    partitions = [
        dg.MultiPartitionKey({"season": season, "gameweek": gw})
        for season in seasons
        for gw in gameweeks
    ]

    print(f"Starting backfill: {len(seasons)} season(s), up to GW{args.max_gw}")
    print(f"Seasons: {', '.join(seasons)}")
    print(f"Total partitions to attempt: {len(partitions)}\n")

    job = defs.get_job_def("fpl_backfill_job")

    succeeded = []
    failed = []

    for partition_key in partitions:
        season = partition_key.keys_by_dimension["season"]
        gameweek = partition_key.keys_by_dimension["gameweek"]
        label = f"{season} {gameweek}"

        print(f"[{label}] Running...")
        try:
            result = job.execute_in_process(partition_key=partition_key)
            if result.success:
                print(f"[{label}] OK")
                succeeded.append(label)
            else:
                print(f"[{label}] FAILED (job returned failure)")
                failed.append(label)
        except Exception as e:
            print(f"[{label}] FAILED ({e})")
            failed.append(label)

    print(f"\nBackfill complete: {len(succeeded)} succeeded, {len(failed)} failed")

    if failed:
        print("Failed partitions:")
        for label in failed:
            print(f"  - {label}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
