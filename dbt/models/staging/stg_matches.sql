WITH source AS (
  SELECT      *
  FROM        ipl_catalog.silver.matches
),

cleaned AS (
  SELECT      match_id,
              season,
              match_date,
              venue,
              city,
              team1,
              team2,
              toss_winner,
              toss_decision,
              winner,
              loser,
              result AS match_result,
              result_margin,
              margin_type,
              toss_winner_won
  FROM        source
  WHERE       match_id IS NOT NULL
)

SELECT * FROM cleaned