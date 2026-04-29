WITH source AS (
  SELECT      *
  FROM        ipl_catalog.silver.players
)

SELECT      player_id,
            player_name,
            team,
            season,
            cricinfo_url,
            valid_from,
            valid_to,
            is_current
FROM        source