with gameweeks as (
    select distinct season, gameweek from {{ ref('stg_matches') }}
), enriched as (
    select
        season,
        gameweek as gameweek_number,
        season = max(season) over() 
         and
        gameweek = max(gameweek) over(partition by season) as is_current
    from gameweeks
)

select * from enriched