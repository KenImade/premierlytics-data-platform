with source as (
   select * from  {{ source('premierlytics', 'player_gameweek_stats_bronze') }}
),

cleaned as (
    select
        first_name,
        second_name,
        web_name,
        status,
        CASE status
            WHEN 'a' THEN 'Available'
            WHEN 'd' THEN 'Doubtful'
            WHEN 'i' THEN 'Injured'
            WHEN 'n' THEN 'Not Available'
            WHEN 's' THEN 'Suspended'
            WHEN 'u' THEN 'Unavailable'
            ELSE 'Unknown'
        END AS status_detailed,
        news,
        news_added,
        now_cost,
        now_cost_rank,
        now_cost_rank_type,
        total_points,
        event_points,
        points_per_game,
        selected_by_percent,
        transfers_in,
        transfers_out,
        minutes,
        goals_scored,
        assists,
        clean_sheets,
        goals_conceded,
        own_goals,
        penalties_saved,
        penalties_missed,
        yellow_cards,
        red_cards,
        saves,
        bonus,
        bps as total_bonus_point_system_score,
        influence,
        creativity,
        threat,
        ict_index,
        expected_goals,
        expected_assists,
        expected_goal_involvements,
        expected_goals_conceded,
        tackles,
        clearances_blocks_interceptions,
        recoveries,
        defensive_contribution,
        season,
        gameweek,
        ingested_at
    from source
)


select * from cleaned