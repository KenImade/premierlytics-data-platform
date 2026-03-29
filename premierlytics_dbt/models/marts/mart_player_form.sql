with player_performances as (
    select * from {{ ref('fact_player_match_performance') }}
),

players as (
    select * from {{ ref('dim_player') }}
),

matches as (
    select * from {{ ref('dim_match') }}
),

-- Step 1: Enrich with dimension attributes
enriched as (
    select
        pp.*,
        p.display_name,
        p.position,
        m.season,
        m.gameweek,
        m.kickoff_time
    from player_performances pp
    left join players p on pp.player_sk = p.player_sk
    left join matches m on pp.match_sk = m.match_sk
),

-- Step 2: Aggregate per player per gameweek
aggregated as (
    select
        player_sk,
        display_name,
        position,
        season,
        gameweek,
        sum(minutes_played) as total_minutes,
        sum(goals) as goals,
        sum(assists) as assists,
        sum(xg) as expected_goals,
        sum(xa) as expected_assists,
        -- Calculate a simple "Involvement" metric
        sum(goals + assists) as goal_involvements
    from enriched
    group by 1, 2, 3, 4, 5
)

select 
    *,
    -- Calculate rolling 3-gameweek goals as a "Form" indicator
    sum(goals) over (
        partition by player_sk 
        order by gameweek 
        rows between 2 preceding and current row
    ) as rolling_3_gw_goals
from aggregated