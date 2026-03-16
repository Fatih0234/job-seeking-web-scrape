import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabaseServer";
import { friendlySupabaseError } from "@/lib/supabaseErrors";
import type { CityBubble } from "@/lib/apiTypes";

export const runtime = "nodejs";

function num(q: URLSearchParams, key: string) {
  const v = q.get(key);
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function int(q: URLSearchParams, key: string, d: number) {
  const v = q.get(key);
  const n = v == null ? d : Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : d;
}

function timeframeMetric(raw: string | null) {
  if (raw === "24h" || raw === "1") return "new_24h_unique";
  if (raw === "30d") return "new_30d_unique";
  if (raw === "all") return "jobs_unique";
  return "new_7d_unique";
}

function cutoffFromTimeframe(raw: string | null) {
  if (raw === "all") return null;
  if (raw === "24h" || raw === "1") return new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  if (raw === "30d") return new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  return new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const q = url.searchParams;

  const nelat = num(q, "nelat");
  const nelon = num(q, "nelon");
  const swlat = num(q, "swlat");
  const swlon = num(q, "swlon");
  if (nelat == null || nelon == null || swlat == null || swlon == null) {
    // In dev, the browser or tooling may hit the endpoint without params.
    // Returning empty avoids noisy logs; the real map request includes bbox.
    return NextResponse.json({ rows: [] });
  }

  const limit = Math.max(10, Math.min(500, int(q, "limit", 350)));
  const timeframe = q.get("days");
  const metric = timeframeMetric(timeframe);

  const supabase = supabaseServer();
  if (timeframe === "30d") {
    const cutoff = cutoffFromTimeframe(timeframe);
    let query = supabase
      .from("working_student_map_points_v")
      .select("platform,job_id,lat,lon,map_city_label,posted_at_utc,first_seen_at")
      .eq("token_kind", "city")
      .gte("lat", swlat)
      .lte("lat", nelat)
      .gte("lon", swlon)
      .lte("lon", nelon)
      .not("lat", "is", null)
      .not("lon", "is", null);

    if (cutoff) {
      query = query.or(`posted_at_utc.gte.${cutoff},first_seen_at.gte.${cutoff}`);
    }

    const { data, error } = await query.range(0, 9999);
    if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });

    const grouped = new Map<string, CityBubble>();
    const seen = new Map<string, Set<string>>();

    for (const row of data ?? []) {
      if (row.lat == null || row.lon == null) continue;
      const bubbleKey = `${row.lat}:${row.lon}:${row.map_city_label}`;
      const jobKey = `${row.platform}:${row.job_id}`;
      let bucketSeen = seen.get(bubbleKey);
      if (!bucketSeen) {
        bucketSeen = new Set<string>();
        seen.set(bubbleKey, bucketSeen);
      }
      if (bucketSeen.has(jobKey)) continue;
      bucketSeen.add(jobKey);

      const current = grouped.get(bubbleKey);
      if (current) {
        current.jobs_unique += 1;
        current.jobs_weighted += 1;
        current.new_30d_unique = (current.new_30d_unique ?? 0) + 1;
        continue;
      }

      grouped.set(bubbleKey, {
        token_kind: "city",
        lat: row.lat,
        lon: row.lon,
        map_city_label: row.map_city_label,
        jobs_unique: 1,
        jobs_weighted: 1,
        new_24h_unique: 0,
        new_7d_unique: 0,
        new_30d_unique: 1,
        top_companies: null,
        top_skills: null,
      });
    }

    const rows = Array.from(grouped.values())
      .sort((a, b) => (b.new_30d_unique ?? 0) - (a.new_30d_unique ?? 0))
      .slice(0, limit);
    return NextResponse.json({ rows });
  }

  const { data, error } = await supabase
    .from("working_student_city_bubbles_v")
    .select(
      "token_kind,lat,lon,map_city_label,jobs_unique,jobs_weighted,new_24h_unique,new_7d_unique,top_companies,top_skills"
    )
    .gte("lat", swlat)
    .lte("lat", nelat)
    .gte("lon", swlon)
    .lte("lon", nelon)
    .gt(metric, 0)
    .order(metric, { ascending: false })
    .limit(limit);

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({
    rows: (data ?? []).map((row) => ({
      ...row,
      new_30d_unique: row.jobs_unique,
    })),
  });
}
