WITH match_score AS (
  SELECT      match_id,
              innings,
              batting_team,
              SUM(total_runs) AS innings_total,
              SUM(is_wicket :: INT) AS wickets_lost,
              SUM(over_number) AS overs_completed
  FROM        {{REF('stg-deliveries')}}
  GROUP BY    match_id,
              innings,
              batting_team
),

first_innings AS (
  SELECT      *
  FROM        match_score
  WHERE       innings = 1
),

second_innings AS (
  SELECT      *
  FROM        match_score
  WHERE       innings = 2
)

SELECT        m.match_id,
              m.season,
              m.match_date,
              m.venue,
              m.city,
              m.team1,
              m.team2,
              m.toss_winner,
              m.toss_decision,
              m.winner,
              m.loser,
              m.match_result,
              m.result_margin,
              m.margin_type,
              m.toss_winner_won,

              -- first innints score
              f1.batting_team AS first_innings_team,
              f1.innings_total AS first_innings_total,
              f1.wickets_lost AS first_innings_wickets,
              f1.overs_completed AS first_innings_overs,

              -- second innings score
              f2.batting_team AS second_innings_team,
              f2.innings_total AS second_innings_total,
              f2.wickets_lost AS second_innings_wickets,
              f2.overs_completed AS second_innings_overs
FROM          {{REF('stg-matches')}} m
LEFT JOIN     first_innings f1
ON            m.match_id = f1.match_id
LEFT JOIN     second_innings f2
ON            m.match_id = f2.match_id