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

  const nelat = num(q, "nelat");
  const nelon = num(q, "nelon");
  const swlat = num(q, "swlat");
  const swlon = num(q, "swlon");
  if (nelat == null || nelon == null || swlat == null || swlon == null) {
    return NextResponse.json({ error: "Missing bbox params" }, { status: 400 });
  }

  const limit = Math.max(50, Math.min(1000, int(q, "limit", 500)));
  const offset = Math.max(0, int(q, "offset", 0));
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
        "token_kind",
        "map_point_id",
        "map_city_label",
        "lat",
        "lon",
        "geocode_status",
        "remote_scatter_ok",
      ].join(",")
    )
    .eq("token_kind", "remote")
    .gte("lat", swlat)
    .lte("lat", nelat)
    .gte("lon", swlon)
    .lte("lon", nelon);

  if (cutoff) {
    query = query.or(`posted_at_utc.gte.${cutoff},first_seen_at.gte.${cutoff}`);
  }

  const { data, error } = await query.range(offset, offset + limit - 1);

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({ rows: data ?? [] });
}
