{{
    config(
        materialized='incremental',
        unique_key=['player_sk', 'gameweek_sk'],
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_playerstats') }}
    {% if is_incremental() %}
    where (season, gameweek) not in (
        select distinct season, gameweek from {{ this }}
    )
    {% endif %}
),

players as (
    select * from {{ ref('dim_player') }}
),

gameweeks as (
    select * from {{ ref('dim_gameweek') }}
),

final as (
    select
        -- Partition columns
        source.season,
        source.gameweek,

        -- Foreign keys
        players.player_sk,
        gameweeks.gameweek_sk,
        players.team_sk,

        -- Pricing & cost
        source.now_cost,
        source.now_cost_rank,
        source.now_cost_rank_type,
        source.cost_change_event,
        source.cost_change_event_fall,
        source.cost_change_start,
        source.cost_change_start_fall,

        -- Ownership & transfers
        source.selected_by_percent,
        source.selected_rank,
        source.selected_rank_type,
        source.transfers_in,
        source.transfers_in_event,
        source.transfers_out,
        source.transfers_out_event,

        -- Points & form
        source.total_points,
        source.event_points,
        source.points_per_game,
        source.points_per_game_rank,
        source.points_per_game_rank_type,
        source.form,
        source.form_rank,
        source.form_rank_type,
        source.value_form,
        source.value_season,
        source.ep_next,
        source.ep_this,
        source.bonus,
        source.bps,
        source.dreamteam_count,

        -- ICT index
        source.influence,
        source.influence_rank,
        source.influence_rank_type,
        source.creativity,
        source.creativity_rank,
        source.creativity_rank_type,
        source.threat,
        source.threat_rank,
        source.threat_rank_type,
        source.ict_index,
        source.ict_index_rank,
        source.ict_index_rank_type,

        -- Expected stats
        source.expected_goals,
        source.expected_assists,
        source.expected_goal_involvements,
        source.expected_goals_conceded,

        -- Per-90 metrics
        source.expected_goals_per_90,
        source.expected_assists_per_90,
        source.expected_goal_involvements_per_90,
        source.expected_goals_conceded_per_90,
        source.saves_per_90,
        source.clean_sheets_per_90,
        source.goals_conceded_per_90,
        source.starts_per_90,
        source.defensive_contribution_per_90,

        -- Availability & status
        source.status,
        source.status_detailed,
        source.news,
        source.news_added,
        source.chance_of_playing_next_round,
        source.chance_of_playing_this_round,

        -- Set pieces
        source.corners_and_indirect_freekicks_order,
        source.corners_and_indirect_freekicks_text,
        source.direct_freekicks_order,
        source.direct_freekicks_text,
        source.penalties_order,
        source.penalties_text,
        source.set_piece_threat,

        -- Cumulative season stats
        source.minutes,
        source.goals_scored,
        source.assists,
        source.clean_sheets,
        source.goals_conceded,
        source.own_goals,
        source.penalties_saved,
        source.penalties_missed,
        source.yellow_cards,
        source.red_cards,
        source.saves,
        source.starts,
        source.defensive_contribution,
        source.tackles,
        source.clearances_blocks_interceptions,
        source.recoveries

    from source
    left join players
        on source.id = players.player_id
        and source.season = players.season
        and source.gameweek >= players.gameweek_effective_from
        and source.gameweek <= players.gameweek_effective_to
    left join gameweeks
        on source.gameweek = gameweeks.gameweek_number
        and source.season = gameweeks.season
)

select * from final
