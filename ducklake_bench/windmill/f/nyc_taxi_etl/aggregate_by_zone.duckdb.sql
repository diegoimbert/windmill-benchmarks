CREATE OR REPLACE TABLE zone_stats AS
SELECT
  PULocationID AS pickup_location_id,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare,
  AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_pct
FROM enriched_trips
GROUP BY PULocationID
ORDER BY trip_count DESC;
