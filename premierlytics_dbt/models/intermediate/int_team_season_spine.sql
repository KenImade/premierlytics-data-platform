{{ config(materialized='table') }}

with teams as (
    select * from {{ ref('stg_teams') }}
),

enriched as (
    select
        team_code,
        team_id,
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
        season = max(season) over() as is_current
    from teams
)


select  * from enriched