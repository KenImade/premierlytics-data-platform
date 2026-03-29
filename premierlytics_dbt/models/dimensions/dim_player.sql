with source as (
    select * from {{ ref('int_player_team_history') }}
),

teams as (
    select * from {{ ref('dim_team') }}
),

with_sk as (
    select
        {{ dbt_utils.generate_surrogate_key(['player_id', 'season', 'gameweek_effective_from']) }} as player_sk,
        *
    from source
),

final as (
    select
        with_sk.player_sk,
        with_sk.player_id,
        with_sk.first_name,
        with_sk.second_name,
        with_sk.display_name,
        with_sk.position,
        with_sk.team_code,
        with_sk.season,
        with_sk.gameweek_effective_from,
        with_sk.gameweek_effective_to,
        with_sk.is_current,
        teams.team_sk,
        teams.season_sk
    from with_sk
    left join teams
        on with_sk.team_code = teams.team_code
        and with_sk.season = teams.season
)

select * from final