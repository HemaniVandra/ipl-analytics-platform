-- Head-to-head record between every pair of teams
WITH match_pairs AS (
  SELECT    -- always put teams in alphabetical order so (MI vs CSK) = (CSK vs MI)
            LEAST(team1, team2) AS team_a,
            GREATEST(team1, team2) AS team_b,
            winner,
            season,
            venue,
            result_margin,
            margin_type,
            match_result

  FROM      {{ref('stg_matches')}}
  WHERE     match_result = 'completed'
),

aggregated AS (
  SELECT    team_a,
            team_b,
            COUNT(*) AS total_matches,
            SUM(CASE WHEN winner = team_a THEN 1 ELSE 0 END) AS team_a_wins,
            SUM(CASE WHEN winner = team_b THEN 1 ELSE 0 END) AS team_b_wins,
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS ties,

            -- average winning margin
            ROUND(AVG(CASE WHEN margin_type = 'runs'
                            THEN result_margin END), 1) AS avg_margin_runs,
            ROUND(AVG(CASE WHEN margin_type = 'wickets'
                            THEN result_margin END), 1) AS avg_margin_wickets,

            -- most recent match
            MAX(season) AS last_season_played
  FROM      match_pairs
  GROUP BY  team_a,
            team_b
)

SELECT      *,
            ROUND(team_a_wins * 100.0 / NULLIF(total_matches, 0), 2) AS team_a_win_pct,
            ROUND(team_b_wins * 100.0 / NULLIF(total_matches, 0), 2) AS team_b_win_pct
FROM        aggregated
ORDER BY    total_matches DESC