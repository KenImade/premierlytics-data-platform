with source as (
    select * from {{ ref('int_team_season_spine') }}
),

seasons as (
    select * from {{ ref('dim_season') }}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['team_id', 'season'])}} as team_sk,
        seasons.season_sk,
        source.team_id,
        source.team_code,
        source.name,
        source.short_name,
        source.pulse_id,
        source.fotmob_name,
        source.season,
        source.is_current,
        source.strength,
        source.strength_overall_home,
        source.strength_overall_away,
        source.strength_attack_home,
        source.strength_attack_away,
        source.strength_defence_home,
        source.strength_defence_away
    from source
    left join seasons
        on source.season = seasons.season_id
)

select * from final