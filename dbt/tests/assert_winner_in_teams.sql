-- Test: Match winner must be one of the two teams playing
-- If this query returns any rows → test FAILS
-- A winner outside team1/team2 means a data standardisation error
-- e.g. team name mismatch after standardise_team()

SELECT      match_id,
            team1,
            team2,
            winner
FROM        ipl_catalog.silver.matches
WHERE       winner IS NOT NULL
AND         result = 'completed'
AND         winner != team1
AND         winner != team2