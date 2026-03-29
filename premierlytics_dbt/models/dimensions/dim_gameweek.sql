with source as (
    select * from {{ref('int_gameweek_spine')}}
),

season as (
    select * from {{ref('dim_season')}}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['gameweek_number', 'season'])}} as gameweek_sk,
        season.season_sk,
        source.season,
        gameweek_number,
        source.is_current
    from source
    left join season
        on source.season = season.season_id
)

select * from final