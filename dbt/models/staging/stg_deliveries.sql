WITH source AS (
  SELECT    *
  FROM      ipl_catalog.silver.deliveries
),

cleaned AS (
  SELECT    match_id,
            season,
            to_date(start_date, 'yyyy-MM-dd') AS match_date,
            venue,
            CAST(innings AS INT) AS innings,

            -- -- parse over and ball from "1.1" format
            -- CAST(split(ball, '\\.')[0] AS INT) AS over_number,
            -- CAST(split(ball, '\\.')[1] AS INT) AS ball_number,

            batting_team,
            bowling_team,
            striker,
            non_striker,
            bowler,

            -- Numeric casts
            COALESCE(CAST(runs_off_bat AS INT), 0) AS runs_off_bat,
            COALESCE(CAST(extras AS INT), 0) AS extras,
            COALESCE(CAST(wides AS INT), 0) AS wides,
            COALESCE(CAST(noballs AS INT), 0) AS noballs,
            COALESCE(CAST(byes AS INT), 0) AS byes,
            COALESCE(CAST(legbyes AS INT), 0) AS legbyes,

            -- Derived fields
            total_runs,
            is_boundary,
            is_six,
            is_dot_ball,
            is_wicket,
            phase
  FROM      source
  WHERE     match_id IS NOT NULL
)

SELECT    *
FROM      cleaned