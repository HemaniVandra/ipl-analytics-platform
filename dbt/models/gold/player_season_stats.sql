-- Batting + bowling stats aggregated per player per season

WITH batting AS (
  SELECT      striker AS player_name,
              season,
              batting_team AS team,
              COUNT(*) AS balls_faced,
              SUM(runs_off_bat) AS runs_scored,
              SUM(CASE WHEN is_boundary THEN 1 ELSE 0 END) AS fours,
              SUM(CASE WHEN is_six THEN 1 ELSE 0 END) AS sixes,
              SUM(CASE WHEN is_dot_ball THEN 1 ELSE 0 END) AS dot_balls_faced,
              COUNT(DISTINCT match_id) AS matches_batted,
              SUM(CASE WHEN is_wicket AND player_dismissed = striker THEN 1 ELSE 0 END) AS dismissals
  FROM        {{ref('stg_deliveries')}}
  WHERE       wides = 0 -- wides don't count as balls faced
  GROUP BY    striker,
              season,
              batting_team
),

batting_with_metrics AS (
  SELECT      *,
              ROUND(runs_scored / NULLIF(balls_faced, 0) * 100, 2) AS strike_rate,
              ROUND(runs_scored / NULLIF(dismissals, 0), 2) AS batting_average,
              ROUND(fours * 100.0 / NULLIF(balls_faced, 0), 2) AS boundary_pct
  FROM        batting
),

bowling AS (
  SELECT      bowler AS player_name,
              season,
              bowling_team AS team,

              -- legal deliveries only (no wides/noballs for over count)
              SUM(CASE WHEN no_balls = 0 AND wides = 0 THEN 1 ELSe 0 END) AS legal_balls,
              SUM(runs_offbat + extras) AS runs_conceded,
              SUM(CASE WHEN is_wicket AND wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field') THEN 1 ELSE 0 END) AS wickets,
              COUNT(DISTINCT match_id) AS matches_bowled,
              SUM(CASE WHEN is_dot_ball THEN 1 ELSE 0 END) AS dot_balls_bowled
  
  FROM        {{ref('stg_deliveries')}}
  GROUP BY    bowler,
              season,
              bowling_team
),

bowling_with_metrics AS (
  SELECT      *,
              ROUND(legal_balls / 6.0, 2) AS overs_bowled,
              ROUND(runs_conceded / NULLIF(legal_balls / 6.0, 0), 2) AS economy_rate,
              ROUND(runs_conceded / NULLIF(wickets, 0), 2) AS bowling_average,
              ROUND(legal_balls / NULLIF(wickets, 0), 2) AS bowling_strike_rate,
              ROUND(dot_balls_bowled * 100.0 / NULLIF(legal_balls, 0), 2) AS dot_ball_pct
  
  FROM        bowling
),

combined AS (
  SELECT          COALESCE(bat.player_name, bowl.player_name) AS player_name,
                  COALESCE(bat.season, bowl.season) AS season,
                  COALESCE(bat.team, bowl.team) AS team,

                  -- batting
                  COALESCE(bat.matches_batted, 0) AS matches_batted,
                  COALESCE(bat.runs_scored, 0) AS runs_scored,
                  COALESCE(bat.balls_faced, 0) AS balls_faced,
                  COALESCE(bat.fours, 0) AS fours,
                  COALESCE(bat.sixes, 0) AS sixes,
                  COALESCE(bat.dismissals, 0) AS dismissals,
                  bat.strike_rate,
                  bat.batting_average,
                  bat.boundary_pct,

                  -- bowling
                  COALESCE(bowl.matches_bowled, 0) AS matches_bowled,
                  COALESCE(bowl.wickets, 0) AS wickets,
                  COALESCE(bowl.runs_conceded) AS runs_conceded,
                  bowl.overs_bowled,
                  bowl.economy_rate,
                  bowl.bowling_average,
                  bowl.bowling_strike_rate,
                  bowl.dot_ball_pct

  FROM            batting_with_metrics bat
  FULL OUTER JOIN bowling_with_metrics bowl
  ON              bat.player_name = bowl.player_name
  AND             bat.season = bowl.season
)

SELECT      *
FROM        combined
ORDER BY    season,
            runs_scored DESC














