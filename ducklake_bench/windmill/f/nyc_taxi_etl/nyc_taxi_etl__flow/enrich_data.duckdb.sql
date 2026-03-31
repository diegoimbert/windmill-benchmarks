ATTACH 'ducklake' AS dl; USE dl;

CREATE OR REPLACE TABLE enriched_trips AS
SELECT
  *,
  DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
  CASE
    WHEN DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 0
    THEN trip_distance / (DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)
    ELSE NULL
  END AS speed_mph,
  CASE
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'night'
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 6 AND 11 THEN 'morning'
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 12 AND 17 THEN 'afternoon'
    ELSE 'evening'
  END AS time_of_day_bucket,
  CASE
    WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN true
    ELSE false
  END AS is_weekend
FROM clean_trips;
