with source as (
    select 
        * 
    from {{ source('premierlytics', 'teams_bronze') }}
    qualify row_number() over (
        partition by code, id, season
        order by season
    ) = 1
),

cleaned as (
    select
        code as team_code,
        id as team_id,
        name,
        short_name,
        strength,
        strength_overall_home,
        strength_overall_away,
        strength_attack_home,
        strength_attack_away,
        strength_defence_home,
        strength_defence_away,
        pulse_id,
        fotmob_name,
        season,
        ingested_at
    from source
)


select * from cleaned