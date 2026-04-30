-- Test: Over numbers must be between 1 and 20 in Silver deliveries
-- If this query returns any rows → test FAILS
-- Cricsheet data occasionally has over 0 or over 21+ due to
-- super overs or data entry errors

SELECT      match_id,
            over_number,
            COUNT(*) AS delivery_count
FROM        ipl_catalog.silver.deliveries
WHERE       over_number < 1
OR          over_number > 20
GROUP BY    match_id, over_number