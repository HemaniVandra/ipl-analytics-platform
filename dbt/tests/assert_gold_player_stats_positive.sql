-- Test: All calculated metrics in Gold player_season_stats
-- must be non-negative
-- If this query returns any rows → test FAILS

SELECT      player_name,
            season,
            strike_rate,
            economy_rate,
            batting_average,
            bowling_average
FROM        ipl_catalog.gold.player_season_stats
WHERE       strike_rate     < 0
OR          economy_rate    < 0
OR          batting_average < 0
OR          bowling_average < 0