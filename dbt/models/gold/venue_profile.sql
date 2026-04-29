WITH delivery_agg AS (
  SELECT      venue,
              season,
              match_id,
              innings,
              SUM(total_runs) AS innings_total,
              SUN(CASE WHEN over_number <= 6
                        THEN total_runs ELSE 0 END) AS powerplay_runs,
              SUM(CASE WHEN over_number >= 15
                        THEN total_runs ELSE 0 END) AS death_runs,
              SUM(CASE WHEN is_six THEN 1 ELSE 0 END) AS sixes,
              SUM(CASE WHEN is_boundary THEN 1 ELSE 0 END) AS boundaries
  FROM        {{REF('stg_deliveries')}}
  GROUP BY    venue,
              season,
              match_id,
              innings
),

venue_match AS (
  SELECT      d.venue,
              d.season,
              d.match_id,
              d.innings,
              d.innings_total,
              d.powerplay_runs,
              d.death_runs,
              d.sixes,
              d.boundaries,
              m.winner,
              m.toss_decision,
              m.toss_winner_won
  FROM        delivery_agg d
  LEFT JOIN   {{REF('stg_matches')}} m
  ON          d.match_id = m.match_id
),

aggregated AS (
  SELECT      venue,
              COUNT(DISTINCT match_id) AS total_matches,
              ROUND(AVG(CASE WHEN innings = 1
                              THEN innings_total END), 1) AS avg_first_innings_total,
              ROUND(AVG(CASE WHEN innings = 2
                              THEN innings_total END), 1) AS avg_second_innings_total,
              MAX(innings_total) AS highest_innings_total,
              ROUND(AVG(powerplay_runs), 1) AS avg_powerplay_score,
              ROUND(AVG(death_runs), 1) AS avg_death_overs_score,
              ROUND(AVG(sixes), 1) AS avg_sixes_per_innings,
              ROUND(
                SUM(CASE WHEN toss_winner_won THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(DISTINCT match_id), 0),
              2) AS toss_win_pct,
              ROUND(
                SUM(CASE WHEN toss_decision = 'field' AND toss_winner_won THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN toss_decision = 'field' THEN 1 ELSE 0 END), 0)
              , 2) AS field_first_win_pct
  FROM        venue_match
  GROUP BY    venue
)

SELECT      *
FROM        aggregated
ORDER BY    total_matches DESC

