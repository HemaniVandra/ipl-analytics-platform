-- Test: Every player must have exactly one active SCD2 record
-- If this query returns any rows → test FAILS
-- Returning rows means a player has multiple is_current=true records
-- which breaks our SCD2 contract

SELECT      player_id,
            player_name,
            COUNT(*) AS active_records
FROM        ipl_catalog.silver.players
WHERE       is_current = true
GROUP BY    player_id, player_name
HAVING      COUNT(*) > 1