CREATE OR REPLACE TABLE hourly_stats AS
SELECT
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance,
  AVG(trip_duration_minutes) AS avg_duration_minutes
FROM enriched_trips
GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
ORDER BY hour_of_day;
