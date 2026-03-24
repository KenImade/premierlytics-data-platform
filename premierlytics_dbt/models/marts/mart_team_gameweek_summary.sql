with performances as (
    select * from {{ ref('fact_match_team_performance') }}
),

matches as (
    select * from {{ ref('dim_match') }}
),

teams as (
    select * from {{ ref('dim_team') }}
),

-- Step 1: Enrich with dimension attributes
enriched as (
    select
        p.*,
        t.name as team_name,
        t.short_name,
        m.season,
        m.gameweek
    from performances p
    left join teams t on p.team_sk = t.team_sk
    left join matches m on p.match_sk = m.match_sk
),

-- Step 2: Aggregate per team per gameweek
aggregated as (
    select
        -- Group by keys
        team_sk,
        gameweek_sk,
        season,
        gameweek,

        -- Team identity
        team_name,
        short_name,

        -- Matches in this gameweek
        count(*) as matches_played,

        -- Result summary
        count(case when score > opponent_score then 1 end) as wins,
        count(case when score = opponent_score then 1 end) as draws,
        count(case when score < opponent_score then 1 end) as losses,
        sum(case 
            when score > opponent_score then 3 
            when score = opponent_score then 1 
            else 0 
        end) as points,

        -- Goals
        sum(score) as goals_for,
        sum(opponent_score) as goals_against,
        sum(score) - sum(opponent_score) as goal_difference,

        -- Aggregate performance stats
        sum(expected_goals_xg) as expected_goals,
        sum(total_shots) as total_shots,
        sum(shots_on_target) as shots_on_target,
        sum(big_chances) as big_chances,
        avg(possession) as avg_possession

    from enriched
    group by
        team_sk,
        gameweek_sk,
        season,
        gameweek,
        team_name,
        short_name
)

select * from aggregated