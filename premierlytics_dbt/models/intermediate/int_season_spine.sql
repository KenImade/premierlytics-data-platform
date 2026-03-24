with seasons as (
    select distinct season from {{ ref('stg_matches') }}
),

enriched as (
    select
        season as season_id,
        replace(season, '-', '/') || ' Season' as season_label,
        season = max(season) over () as is_current
    from seasons
)

select * from enriched