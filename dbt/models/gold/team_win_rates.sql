WITH match_results AS (
  SELECT    winner              AS team,
            season,
            venue,
            toss_decision,
            toss_winner_won,
            match_result,
            'win'               AS outcome
  FROM      {{ref('stg_matches')}}
  WHERE     match_result = 'completed'
  AND       winner IS NOT NULL

  UNION ALL

  -- add the losing team's record
  SELECT      loser             AS team,
              season,
              venue,
              toss_decision,
              toss_winner_won,
              match_result,
              'loss'            AS outcome
  FROM        {{ref('stg_matches')}}
  WHERE       match_result = 'completed'
  AND         loser IS NOT NULL
),

aggregated AS (
  SELECT      team,
              season,
              COUNT(*) AS total_matches,
              SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
              ROUND(
                SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) 
                * 100.0 / NULLIF(COUNT(*), 0),
              2) AS win_pct,

              -- toss impact
              SUM(CASE WHEN toss_winner_won AND outcome = 'win'
                        THEN 1 ELSE 0 END
              ) AS toss_win_match_wins,
              ROUND(
                SUM(CASE WHEN toss_winner_won AND outcome = 'win' THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN toss_winner_won THEN 1 ELSE 0 END), 0),
              2) AS win_pct_after_toss_win,

              -- toss decision breakdown
              SUM(CASE WHEN toss_decision = 'bat' AND outcome = 'win' THEN 1 ELSE 0 END) AS wins_batting_first,
              SUM(CASE WHEN toss_decision = 'field' AND outcome = 'win' THEN 1 ELSE 0 END) AS wins_fielding_first

  FROM        match_results
  GROUP BY    team,
              season
)

SELECT      *
FROM        aggregated
ORDER BY    season,
            win_pct DESC
