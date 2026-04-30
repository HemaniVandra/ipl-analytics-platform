-- models/gold/venue_avg_scores.sql
-- Average scores and match stats per venue
-- This is what was listed in week4 deliverables

WITH delivery_agg AS (
  SELECT      venue,
              match_id,
              innings,
              batting_team,

              -- runs by phase
              SUM(total_runs) AS innings_total,
              SUM(CASE WHEN over_number <= 6 THEN total_runs ELSE 0 END) AS powerplay_runs,
              SUM(CASE WHEN over_number BETWEEN 7 AND 15 THEN total_runs ELSE 0 END) AS middle_runs,
              SUM(CASE WHEN over_number >=  15 THEN total_runs ELSE 0 END) AS death_runs,

              -- Boundaries
              SUM(CASE WHEN is_six THEN 1 ELSE 0 END) AS sixes,
              SUM(CASE WHEN is_boundary THEN 1 ELSE 0 END) AS boundaries,

              -- Wickets
              SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wickets_fallen
  FROM        {{ref('stg_deliveries')}}
  GROUP BY    venue,
              match_id,
              innings,
              batting_team
),

first_innings AS (
  SELECT      *
  FROM        delivery_agg
  WHERE       innings = 1
),

second_innings AS (
  SELECT      *
  FROM        delivery_agg
  WHERE       innings = 2
),

venue_match_summary AS (
  SELECT      f1.venue,
              f1.match_id,
              f1.innings_total AS first_innings_total,
              f1.powerplay_runs AS first_innings_powerplay,
              f1.middle_runs AS first_innings_middle,
              f1.death_runs AS first_innings_death,
              f1.sixes AS first_innings_sixes,
              f1.boundaries AS first_innings_boundaries,
              f1.wickets_fallen AS first_innings_wickets,
              f2.innings_total AS second_innings_total,
              f2.powerplay_runs AS second_innings_powerplay,
              m.winner,
              m.toss_decision,
              m.toss_winner_won,
              m.is_dls,
              m.season
  FROM        first_innings f1
  LEFT JOIN   second_innings f2
  ON          f1.match_id = f2.match_id
  LEFT JOIN   {{ref('stg_matches')}} m
  ON          f1.match_id = m.match_id
),

aggregated AS (
  SELECT      venue,
              
              -- match counts
              COUNT(DISTINCT match_id) AS total_matches,
              COUNT(DISTINCT season) AS seasons,

              -- first innings averages
              ROUND(AVG(first_innings_total), 1) AS avg_first_innings_total,
              ROUND(AVG(first_innings_powerplay), 1) AS avg_first_innings_powerplay,
              ROUND(AVG(first_innings_middle), 1) AS avg_first_innings_middle,
              ROUND(AVG(first_innings_death), 1) AS avg_first_innings_death,
              MAX(first_innings_total) AS highest_first_innings_total,
              MIN(first_innings_total) AS lowest_first_innings_total,

              -- second innings averages
              ROUND(AVG(second_innings_total), 1) AS avg_second_innings_total,
              ROUND(AVG(second_innings_powerplay), 1) AS avg_second_innings_powerplay,
              MAX(second_innings_total) AS highest_second_innings_total,
              
              -- Boundary stats
              ROUND(AVG(first_innings_sixes), 1) AS avg_sixes_per_innings,
              ROUND(AVG(first_innings_boundaries), 1) AS avg_boundaries_per_innings,
              
              -- toss impact
              ROUND(
                SUM(CASE WHEN toss_winner_won THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2
              ) AS toss_win_match_win_pct,

              -- batting first vs second win rate
              ROUND(
                SUM(CASE WHEN toss_decision = 'bat' AND toss_winner_won THEN 1 ELSE 0 END) 
                * 100.0 / NULLIF(
                  SUM(CASE WHEN toss_decision = 'bat' THEN 1 ELSE 0 END), 0), 2
              ) AS bat_first_win_pct,

              ROUND(
                SUM(CASE WHEN toss_decision = 'field' AND toss_winner_won THEN 1 ELSE 0 END) 
                * 100.0 / NULLIF(
                  SUM(CASE WHEN toss_decision = 'field' THEN 1 ELSE 0 END), 0), 2
              ) AS field_first_win_pct,

              -- DLS matches
              SUM(CASE WHEN is_dls THEN 1 ELSE 0 END) AS dls_matches
  FROM        venue_match_summary
  GROUP BY    venue
)

SELECT       *
FROM          aggregated
ORDER BY      total_matches DESC