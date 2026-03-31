CREATE OR REPLACE TABLE clean_trips AS
SELECT *
FROM raw_trips
WHERE passenger_count > 0
  AND fare_amount >= 0
  AND trip_distance > 0
  AND PULocationID IS NOT NULL
  AND DOLocationID IS NOT NULL;
