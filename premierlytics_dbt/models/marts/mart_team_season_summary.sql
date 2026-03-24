with gameweek_summaries as (
    select * from {{ ref('mart_team_gameweek_summary') }}
),

-- Step 1: Aggregate across all gameweeks in a season
aggregated as (
    select
        team_sk,
        team_name,
        short_name,
        season,

        -- League table basics
        sum(matches_played) as played,
        sum(wins) as won,
        sum(draws) as drawn,
        sum(losses) as lost,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goal_difference) as goal_difference,
        sum(points) as points,

        -- Advanced stats
        sum(expected_goals) as total_expected_goals,
        avg(avg_possession) as season_avg_possession

    from gameweek_summaries
    group by
        team_sk,
        team_name,
        short_name,
        season
),

-- Step 2: Add league position
ranked as (
    select
        *,
        rank() over (
            partition by season 
            order by points desc, goal_difference desc, goals_for desc
        ) as league_position
    from aggregated
)

select * from ranked