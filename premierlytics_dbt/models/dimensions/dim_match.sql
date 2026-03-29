with source as (
    select * from {{ ref('stg_matches') }}
),

teams as (
    select * from {{ ref('dim_team') }}
),

gameweeks as (
    select * from {{ ref('dim_gameweek') }}
),

seasons as (
    select * from {{ ref('dim_season') }}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['match_id'])}} as match_sk,
        home_team.team_sk as home_team_sk,
        away_team.team_sk as away_team_sk,
        gameweeks.gameweek_sk,
        seasons.season_sk,
		source.gameweek,
		source.season,
		source.kickoff_time,
		source.home_team,
		source.home_team_elo,
		source.home_score,
		source.away_score,
		source.away_team,
		source.away_team_elo,
		source.finished,
		source.match_id,
		source.match_url,
		source.fotmob_id,
		source.stats_processed,
		source.player_stats_processed
    from source
    left join teams as home_team
        on source.home_team = home_team.team_code
        and source.season = home_team.season
    left join teams as away_team
        on source.away_team = away_team.team_code
        and source.season = away_team.season
    left join gameweeks
        on source.gameweek = gameweeks.gameweek_number
        and source.season = gameweeks.season
    left join seasons
        on source.season = seasons.season_id
)

select * from final