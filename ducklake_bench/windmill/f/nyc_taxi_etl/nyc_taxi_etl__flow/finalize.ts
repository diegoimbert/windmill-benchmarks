import * as wmill from "npm:windmill-client@1";

export async function main() {
  const sql = wmill.ducklake();

  const rawCount = await sql`SELECT COUNT(*) AS cnt FROM raw_trips`.fetchOne();
  const cleanCount = await sql`SELECT COUNT(*) AS cnt FROM clean_trips`.fetchOne();
  const enrichedCount =
    await sql`SELECT COUNT(*) AS cnt FROM enriched_trips`.fetchOne();
  const hourlyCount =
    await sql`SELECT COUNT(*) AS cnt FROM hourly_stats`.fetchOne();
  const zoneCount =
    await sql`SELECT COUNT(*) AS cnt FROM zone_stats`.fetchOne();

  return {
    raw_trips: rawCount.cnt,
    clean_trips: cleanCount.cnt,
    enriched_trips: enrichedCount.cnt,
    hourly_stats: hourlyCount.cnt,
    zone_stats: zoneCount.cnt,
  };
}
