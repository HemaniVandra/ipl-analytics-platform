-- Test: Runs off bat must never be negative in Silver deliveries
-- If this query returns any rows → test FAILS
-- Negative runs indicate a casting or cleaning error in clean_deliveries()

SELECT      match_id,
            innings,
            over_number,
            ball_number,
            striker,
            runs_off_bat
FROM        ipl_catalog.silver.deliveries
WHERE       runs_off_bat < 0