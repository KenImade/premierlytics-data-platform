with player_snapshots as (
    select
        player_id,
        first_name,
        second_name,
        official_fpl_display_name as display_name,
        official_fpl_position as position,
        official_fpl_team_code as team_code,
        season,
        gameweek
    from {{ ref('stg_players') }}
),

-- Step 1: For each player-season, detect when team or position changes
-- Use LAG to compare current row with previous gameweek
change_detection as (
    -- YOUR CODE HERE
    select
        *,
        case
            when lag(team_code) over (
                partition by player_id, season
                order by gameweek
            ) is null then 0
            when team_code != lag(team_code) over (
                partition by player_id, season
                order by gameweek
            ) then 1
            when position != lag(position) over (
                partition by player_id, season
                order by gameweek
            ) then 1
            else 0
        end as is_change
    from player_snapshots
),

-- -- Step 2: Create a group identifier for consecutive same-team-same-position periods
-- -- Use a running SUM of the change flag to create groups
period_groups as (
    -- YOUR CODE HERE
    select
        *,
        sum(is_change) over (
            partition by player_id, season
            order by gameweek
            rows unbounded preceding
        ) as period_group
    from change_detection
),

-- -- Step 3: Aggregate each group to get the effective date range
periods as (
    select
        player_id,
        min(first_name) as first_name,
        min(second_name) as second_name,
        min(display_name) as display_name,
        min(position) as position,
        min(team_code) as team_code,
        season,
        min(gameweek) as gameweek_effective_from,
        max(gameweek) as gameweek_effective_to,
        period_group
    from period_groups
    group by player_id, season, period_group
),
periods_adjusted as (
    select
        player_id,
        first_name,
        second_name,
        display_name,
        position,
        team_code,
        season,
        gameweek_effective_from,
        case
            when row_number() over (
                partition by player_id, season
                order by gameweek_effective_from desc
            ) = 1
            then 38
            else gameweek_effective_to
        end as gameweek_effective_to,
        period_group
    from periods
),

final as (
    select
        player_id,
        first_name,
        second_name,
        display_name,
        position,
        team_code,
        season,
        gameweek_effective_from,
        gameweek_effective_to,
        row_number() over (
            partition by player_id
            order by season desc, gameweek_effective_from desc
        ) = 1 as is_current
    from periods_adjusted
)

select * from final

