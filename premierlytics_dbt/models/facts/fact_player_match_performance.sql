{{
    config(
        materialized='incremental',
        unique_key=['player_sk', 'match_sk'],
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_playermatchstats') }}
),

matches as (
    select * from {{ ref('dim_match') }}
),

players as (
    select * from {{ ref('dim_player') }}
),

final as (
    select
        -- Partition columns
        matches.season,
        matches.gameweek,

        -- Foreign keys
        players.player_sk,
        matches.match_sk,
        players.team_sk,

        -- Attacking
        source.minutes_played,
        source.goals,
        source.assists,
        source.xg,
        source.xa,
        source.xgot,
        source.total_shots,
        source.shots_on_target,
        source.big_chances_missed,
        source.successful_dribbles,
        source.successful_dribbles_percent,
        source.touches,
        source.touches_opposition_box,
        source.penalties_scored,
        source.penalties_missed,
        source.offsides,
        source.corners,

        -- Passing & creativity
        source.accurate_passes,
        source.accurate_passes_percent,
        source.chances_created,
        source.final_third_passes,
        source.accurate_crosses,
        source.accurate_crosses_percent,
        source.accurate_long_balls,
        source.accurate_long_balls_percent,

        -- Defending
        source.tackles_won,
        source.tackles_won_percent,
        source.tackles,
        source.interceptions,
        source.recoveries,
        source.blocks,
        source.clearances,
        source.headed_clearances,
        source.dribbled_past,
        source.defensive_contributions,

        -- Duels
        source.duels_won,
        source.duels_lost,
        source.aerial_duels_won,
        source.aerial_duels_won_percent,
        source.ground_duels_won,
        source.ground_duels_won_percent,

        -- Fouls
        source.fouls_committed,
        source.was_fouled,
        source.dispossessed,

        -- Goalkeeper
        source.saves,
        source.saves_inside_box,
        source.goals_conceded,
        source.team_goals_conceded,
        source.xgot_faced,
        source.goals_prevented,
        source.sweeper_actions,
        source.gk_accurate_passes,
        source.gk_accurate_long_balls,
        source.high_claim,

        -- Physical
        source.top_speed,
        source.distance_covered,
        source.walking_distance,
        source.running_distance,
        source.sprinting_distance,
        source.number_of_sprints,

        -- Match time
        source.start_min,
        source.finish_min

    from source
    inner join matches
        on source.match_id = matches.match_id
    left join players
        on source.player_id = players.player_id
        and matches.season = players.season
        and matches.gameweek >= players.gameweek_effective_from
        and matches.gameweek <= players.gameweek_effective_to

    {% if is_incremental() %}
    where (matches.season, matches.gameweek) not in (
        select distinct season, gameweek from {{ this }}
    )
    {% endif %}
)

select * from final
