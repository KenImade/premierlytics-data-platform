import dagster as dg


season_partitions = dg.StaticPartitionsDefinition(["2024-2025", "2025-2026"])

gameweek_partitions = dg.StaticPartitionsDefinition([f"GW{gw}" for gw in range(1, 39)])

matches_partitions = dg.MultiPartitionsDefinition(
    {
        "season": season_partitions,
        "gameweek": gameweek_partitions,
    }
)
