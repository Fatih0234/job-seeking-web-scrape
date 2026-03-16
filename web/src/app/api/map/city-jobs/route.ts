import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabaseServer";
import { friendlySupabaseError } from "@/lib/supabaseErrors";

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

function cutoffFromTimeframe(raw: string | null) {
  if (raw === "all") return null;
  const days = raw === "24h" || raw === "1" ? 1 : raw === "30d" ? 30 : 7;
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const q = url.searchParams;

  const lat = num(q, "lat");
  const lon = num(q, "lon");
  if (lat == null || lon == null) {
    return NextResponse.json({ error: "Missing lat/lon params" }, { status: 400 });
  }

  const limit = Math.max(10, Math.min(200, int(q, "limit", 50)));
  const offset = Math.max(0, int(q, "offset", 0));
  const eps = 1e-7;
  const cutoff = cutoffFromTimeframe(q.get("days"));

  const supabase = supabaseServer();
  let query = supabase
    .from("working_student_map_points_v")
    .select(
      [
        "platform",
        "job_id",
        "job_url",
        "job_title",
        "company_name",
        "job_location",
        "posted_at_utc",
        "first_seen_at",
        "extracted_skills",
        "token_kind",
        "map_point_id",
        "map_city_label",
        "lat",
        "lon",
        "geocode_status",
        "remote_scatter_ok",
      ].join(",")
    )
    .eq("token_kind", "city")
    .gte("lat", lat - eps)
    .lte("lat", lat + eps)
    .gte("lon", lon - eps)
    .lte("lon", lon + eps);

  if (cutoff) {
    query = query.or(`posted_at_utc.gte.${cutoff},first_seen_at.gte.${cutoff}`);
  }

  const { data, error } = await query
    .order("posted_at_utc", { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({ rows: data ?? [] });
}
