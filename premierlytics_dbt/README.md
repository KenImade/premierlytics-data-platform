# premierlytics_dbt

dbt project for transforming Premier League bronze data in DuckDB into a dimensional model. Sits downstream of the Dagster pipeline which loads raw data into bronze tables.

## Model architecture

```
staging → intermediate → dimensions → facts → marts
 (views)    (tables)      (tables)   (tables) (tables)
```

### Staging
Cleans and lightly transforms the 7 bronze source tables. Views only — no storage cost. Applies deduplication (`QUALIFY`) where needed and maps status codes to human-readable labels.

| Model | Source table |
|---|---|
| `stg_matches` | `matches_bronze` |
| `stg_players` | `players_bronze` |
| `stg_teams` | `teams_bronze` |
| `stg_playermatchstats` | `playermatchstats_bronze` |
| `stg_playerstats` | `playerstats_bronze` |
| `stg_fixtures` | `fixtures_bronze` |
| `stg_player_gameweek_stats` | `player_gameweek_stats_bronze` |

### Intermediate
Prepares spines and restructures data for the dimensional layer.

| Model | Description |
|---|---|
| `int_season_spine` | One row per distinct season |
| `int_gameweek_spine` | One row per season/gameweek combination |
| `int_team_season_spine` | One row per team per season with strength ratings |
| `int_match_team_unpivoted` | Unpivots wide match rows into 2 rows per match (home + away) |
| `int_player_team_history` | SCD Type 2 — groups consecutive periods where a player held the same team/position |

### Dimensions
Surrogate keys generated via `dbt_utils.generate_surrogate_key`.

| Model | Grain | Notes |
|---|---|---|
| `dim_season` | 1 row per season | |
| `dim_gameweek` | 1 row per season/gameweek | FK → `dim_season` |
| `dim_team` | 1 row per team per season | FK → `dim_season` |
| `dim_player` | 1 row per player per effective period | SCD Type 2 — `gameweek_effective_from/to`, `is_current` flag |
| `dim_match` | 1 row per match | FKs → `dim_team` (home + away), `dim_gameweek`, `dim_season` |

### Facts
Incremental tables using `delete+insert` strategy keyed on `(season, gameweek)`. Only the new gameweek is processed on each daily run. All three tables include `season` and `gameweek` as degenerate dimensions for efficient partition pruning.

| Model | Grain | Unique key |
|---|---|---|
| `fact_match_team_performance` | 1 row per team per match | `(match_sk, team_sk)` |
| `fact_player_match_performance` | 1 row per player per match | `(player_sk, match_sk)` |
| `fact_player_gameweek_snapshot` | 1 row per player per gameweek | `(player_sk, gameweek_sk)` |

`fact_player_match_performance` and `fact_player_gameweek_snapshot` use SCD2-aware range joins to `dim_player` to correctly assign team for transferred players.

### Marts
Full table rebuilds — these use window functions across gameweeks so incremental is not suitable.

| Model | Grain | Notes |
|---|---|---|
| `mart_team_gameweek_summary` | 1 row per team per gameweek | Handles double gameweeks by summing |
| `mart_team_season_summary` | 1 row per team per season | League position via `rank()` |
| `mart_player_form` | 1 row per player per gameweek | 3GW rolling goal sum |
| `mart_player_season_summary` | 1 row per player per season | Per-90 metrics, full attacking/defensive breakdown |

## Running locally

Ensure DuckDB is populated (Dagster pipeline must have run first), then from this directory:

```bash
# Run all models and tests
dbt build

# Run models only
dbt run

# Run tests only
dbt test

# Check source freshness (warns after 30h, errors after 48h)
dbt source freshness

# First-time run or after schema changes to fact tables
dbt build --full-refresh
```

## Incremental vs full-refresh

On a normal daily run, only fact table rows for the current gameweek are inserted. Marts are always fully rebuilt.

If you need to reprocess historical data (e.g. after a schema change to a fact table), run:

```bash
dbt build --full-refresh
```

After running `fpl_backfill_job` in Dagster (which loads all historical gameweeks into bronze tables), trigger `fpl_dbt_job` from the Dagster UI to rebuild the full dimensional model in one shot.

## Orchestration

dbt is orchestrated by Dagster via `dagster-dbt`. The `premierlytics_dbt_assets` asset in the Dagster pipeline calls `dbt build` after each pipeline run. Source table dependencies are declared in `stg_sources.yml` using `meta.dagster.asset_key` so Dagster tracks lineage from bronze tables through dbt.

Three Dagster jobs are available:

| Job | When to use |
|---|---|
| `fpl_pipeline_job` | Daily run — processes current gameweek end-to-end including dbt |
| `fpl_backfill_job` | Loads historical gameweeks into bronze (excludes dbt) |
| `fpl_dbt_job` | Run dbt manually after a backfill |

## Packages

| Package | Version | Usage |
|---|---|---|
| `dbt-labs/dbt_utils` | `1.3.3` | `generate_surrogate_key`, `unique_combination_of_columns` tests |
