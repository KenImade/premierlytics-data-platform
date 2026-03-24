with source as (
    select * from {{ ref('int_match_team_unpivoted') }}
),

teams as (
    select * from {{ ref('dim_team') }}
),

matches as (
    select * from {{ ref('dim_match') }}
),

gameweeks as (
    select * from {{ ref('dim_gameweek') }}
),

final as (
    select
        -- Foreign keys
        matches.match_sk,
        teams.team_sk,
        gameweeks.gameweek_sk,

        -- Degenerate dimension
        source.is_home,

        -- Scores
        source.score,
        source.opponent_score,

        -- Possession & xG
        source.possession,
        source.expected_goals_xg,
        source.xg_open_play,
        source.xg_set_play,
        source.non_penalty_xg,
        source.xg_on_target_xgot,

        -- Shots
        source.total_shots,
        source.shots_on_target,
        source.shots_off_target,
        source.blocked_shots,
        source.shots_inside_box,
        source.shots_outside_box,
        source.big_chances,
        source.big_chances_missed,
        source.hit_woodwork,

        -- Passing
        source.passes,
        source.accurate_passes,
        source.accurate_passes_pct,
        source.own_half,
        source.opposition_half,
        source.accurate_long_balls,
        source.accurate_crosses,
        source.accurate_crosses_pct,

        -- Set pieces & other
        source.corners,
        source.throws,
        source.fouls_committed,
        source.touches_in_opposition_box,
        source.offsides,

        -- Discipline
        source.yellow_cards,
        source.red_cards,

        -- Defending
        source.tackles_won,
        source.tackles_won_pct,
        source.interceptions,
        source.blocks,
        source.clearances,
        source.keeper_saves,

        -- Duels
        source.duels_won,
        source.ground_duels_won,
        source.ground_duels_won_pct,
        source.aerial_duels_won,
        source.aerial_duels_won_pct,

        -- Dribbling
        source.successful_dribbles,
        source.successful_dribbles_pct

    from source
    left join teams
        on source.team_name = teams.team_code
        and source.season = teams.season
    left join matches
        on source.match_id = matches.match_id
    left join gameweeks
        on source.gameweek = gameweeks.gameweek_number
        and source.season = gameweeks.season
)

select * from final