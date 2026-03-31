import * as wmill from "npm:windmill-client@1";

export async function main() {
  const sql = wmill.ducklake();

  await sql`
    INSTALL httpfs;
    LOAD httpfs;
  `.execute();

  await sql`
    CREATE OR REPLACE TABLE raw_trips AS
    SELECT *
    FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet');
  `.execute();

  const count = await sql`SELECT COUNT(*) AS cnt FROM raw_trips`.fetchOne();
  return { raw_trips_count: count.cnt };
}
