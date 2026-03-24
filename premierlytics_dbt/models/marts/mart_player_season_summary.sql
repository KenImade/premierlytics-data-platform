with performances as (
    select * from {{ ref('fact_player_match_performance') }}
),

players as (
    select * from {{ ref('dim_player') }}
),

teams as (
    select * from {{ ref('dim_team') }}
),

matches as (
    select * from {{ ref('dim_match') }}
),

-- Step 1: Join fact to dimensions to get season, player name, team name
enriched as (
    select
        p.player_sk,
        p.player_id,
        p.first_name,
        p.second_name,
        p.display_name,
        p.position,
        t.team_sk,
        t.name as team_name,
        t.short_name as team_short_name,
        m.season,
        m.season_sk,

        -- Match-level metrics
        perf.minutes_played,
        perf.start_min,
        perf.finish_min,

        -- Attacking
        perf.goals,
        perf.assists,
        perf.xg,
        perf.xa,
        perf.xgot,
        perf.total_shots,
        perf.shots_on_target,
        perf.big_chances_missed,
        perf.successful_dribbles,
        perf.touches,
        perf.touches_opposition_box,
        perf.penalties_scored,
        perf.penalties_missed,
        perf.offsides,
        perf.corners,

        -- Passing & creativity
        perf.accurate_passes,
        perf.chances_created,
        perf.final_third_passes,
        perf.accurate_crosses,
        perf.accurate_long_balls,

        -- Defending
        perf.tackles_won,
        perf.tackles,
        perf.interceptions,
        perf.recoveries,
        perf.blocks,
        perf.clearances,
        perf.headed_clearances,
        perf.dribbled_past,
        perf.defensive_contributions,

        -- Duels
        perf.duels_won,
        perf.duels_lost,
        perf.aerial_duels_won,
        perf.ground_duels_won,

        -- Discipline
        perf.fouls_committed,
        perf.was_fouled,
        perf.dispossessed,

        -- Goalkeeper
        perf.saves,
        perf.saves_inside_box,
        perf.goals_conceded,
        perf.xgot_faced,
        perf.goals_prevented,
        perf.sweeper_actions,
        perf.high_claim,

        -- Physical
        perf.distance_covered,
        perf.number_of_sprints,
        perf.sprinting_distance

    from performances perf
    inner join matches m
        on perf.match_sk = m.match_sk
    left join players p
        on perf.player_sk = p.player_sk
    left join teams t
        on perf.team_sk = t.team_sk
),

-- Step 2: Aggregate per player per season
aggregated as (
    select
        -- Group by keys
        player_sk,
        player_id,
        season,
        season_sk,
        team_sk,

        -- Player identity
        first_name,
        second_name,
        display_name,
        first_name || ' ' || second_name as full_name,
        position,
        team_name,
        team_short_name,

        -- Appearances
        count(*) as matches_played,
        count(case when start_min = 0 then 1 end) as starts,
        count(case when start_min > 0 then 1 end) as substitute_appearances,

        -- Minutes
        sum(minutes_played) as total_minutes,

        -- Attacking totals
        sum(goals) as total_goals,
        sum(assists) as total_assists,
        sum(xg) as total_xg,
        sum(xa) as total_xa,
        sum(xgot) as total_xgot,
        sum(total_shots) as total_shots,
        sum(shots_on_target) as total_shots_on_target,
        sum(big_chances_missed) as total_big_chances_missed,
        sum(successful_dribbles) as total_successful_dribbles,
        sum(touches) as total_touches,
        sum(touches_opposition_box) as total_touches_opposition_box,
        sum(penalties_scored) as total_penalties_scored,
        sum(penalties_missed) as total_penalties_missed,
        sum(offsides) as total_offsides,
        sum(corners) as total_corners,

        -- Passing & creativity totals
        sum(accurate_passes) as total_accurate_passes,
        sum(chances_created) as total_chances_created,
        sum(final_third_passes) as total_final_third_passes,
        sum(accurate_crosses) as total_accurate_crosses,
        sum(accurate_long_balls) as total_accurate_long_balls,

        -- Per-90 metrics
        round((sum(goals) / nullif(sum(minutes_played), 0)) * 90, 2) as goals_per_90,
        round((sum(assists) / nullif(sum(minutes_played), 0)) * 90, 2) as assists_per_90,
        round((sum(xg) / nullif(sum(minutes_played), 0)) * 90, 2) as xg_per_90,
        round((sum(xa) / nullif(sum(minutes_played), 0)) * 90, 2) as xa_per_90,
        round((sum(xgot) / nullif(sum(minutes_played), 0)) * 90, 2) as xgot_per_90,
        round((sum(total_shots) / nullif(sum(minutes_played), 0)) * 90, 2) as shots_per_90,
        round((sum(chances_created) / nullif(sum(minutes_played), 0)) * 90, 2) as chances_created_per_90,
        round((sum(touches_opposition_box) / nullif(sum(minutes_played), 0)) * 90, 2) as touches_opp_box_per_90,

        -- Defensive totals
        sum(tackles_won) as total_tackles_won,
        sum(tackles) as total_tackles,
        sum(interceptions) as total_interceptions,
        sum(recoveries) as total_recoveries,
        sum(blocks) as total_blocks,
        sum(clearances) as total_clearances,
        sum(headed_clearances) as total_headed_clearances,
        sum(dribbled_past) as total_dribbled_past,
        sum(defensive_contributions) as total_defensive_contributions,

        -- Defensive per-90
        round((sum(tackles_won) / nullif(sum(minutes_played), 0)) * 90, 2) as tackles_won_per_90,
        round((sum(interceptions) / nullif(sum(minutes_played), 0)) * 90, 2) as interceptions_per_90,

        -- Duels
        sum(duels_won) as total_duels_won,
        sum(duels_lost) as total_duels_lost,
        sum(aerial_duels_won) as total_aerial_duels_won,
        sum(ground_duels_won) as total_ground_duels_won,
        round(
            sum(duels_won) / nullif(sum(duels_won) + sum(duels_lost), 0) * 100,
            1
        ) as duel_win_rate_pct,

        -- Discipline
        sum(fouls_committed) as total_fouls_committed,
        sum(was_fouled) as total_was_fouled,
        sum(dispossessed) as total_dispossessed,

        -- Goalkeeper
        sum(saves) as total_saves,
        sum(saves_inside_box) as total_saves_inside_box,
        sum(goals_conceded) as total_goals_conceded,
        sum(xgot_faced) as total_xgot_faced,
        sum(goals_prevented) as total_goals_prevented,
        sum(sweeper_actions) as total_sweeper_actions,
        sum(high_claim) as total_high_claims,

        -- Physical
        sum(distance_covered) as total_distance_covered,
        sum(number_of_sprints) as total_sprints,
        sum(sprinting_distance) as total_sprinting_distance

    from enriched
    group by
        player_sk,
        player_id,
        season,
        season_sk,
        team_sk,
        first_name,
        second_name,
        display_name,
        position,
        team_name,
        team_short_name
)

select * from aggregated