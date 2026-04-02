-- Read-only diagnostic query.
-- Lists vehicles with non-zero coordinates and missing last_known_address.
-- Ordered by last_updated (oldest first).
SELECT
    vehicle_id,
    truck_number,
    last_lat,
    last_lon,
    last_updated,
    CASE
        WHEN last_updated IS NULL
            OR last_updated < (NOW() - INTERVAL '24 hours')
            THEN 'no_fresh_events'
        ELSE 'fresh_event_missing_address'
    END AS diagnosis_bucket
FROM fleet_status_monitor
WHERE COALESCE(last_lat, 0) != 0
  AND COALESCE(last_lon, 0) != 0
  AND last_known_address IS NULL
ORDER BY last_updated ASC NULLS FIRST;
