with source as (
    select * from {{ source('premierlytics', 'players_bronze') }}
),

cleaned as (
    select
        player_id,
        first_name,
        second_name,
        web_name as official_fpl_display_name,
        team_code as official_fpl_team_code,
        CASE 
            WHEN position = 'Unknown' Then 'Manager'
            ELSE position
        END as official_fpl_position,
        gameweek,
        season,
        ingested_at
    from source
)


select * from cleaned