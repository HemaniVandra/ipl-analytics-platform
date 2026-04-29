-- economy rates split by phase: powerplay /  middle / death

with phase_stats AS (
  SELECT    bowler,
            season,
            bowling_team AS team,
            phase,
            COUNT(DISTINCT match_id) AS matches,
            SUM(CASE WHEN no_balls = 0 AND wides = 0 THEN 1 ELSE 0 END) AS legal_balls,
            SUM(runs_off_bat + extras) AS runs_conceded,
            SUM(CASE WHEN is_wicket AND wicket_type NOT IN (
              'run out',
              'retired hurt',
              'obstructing the field'
            ) THEN 1 ELSE 0 END) AS wickets,
            SUM(CASE WHEN is_dot_ball THEN 1 ELSE 0 END) AS dot_balls
  FROM      {{ref('stg_deliveries')}}
  WHERE     phase IS NOT NULL
  GROUP BY  bowler,
            season,
            bowling_team,
            phase
),

with_metrics AS (
  SELECT    *,
            ROUND(legal_balls / 6.0, 2) AS overs,
            ROUND(runs_conceded / NULLIF(legal_balls / 6.0, 0), 2) AS economy_rate,
            ROUND(dot_balls * 100.0 / NULLIF(legal_balls, 0), 2) AS dot_ball_pct
  FROM      phase_stats
  WHERE     legal_balls >= 6 -- at least 1 over bowled in this phase
)

SELECT      *
FROM        with_metrics
ORDER BY    season,
            phase,
            economy_rate