import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabaseServer";
import { friendlySupabaseError } from "@/lib/supabaseErrors";

export const runtime = "nodejs";

function int(q: URLSearchParams, key: string, d: number) {
  const v = q.get(key);
  const n = v == null ? d : Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : d;
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const days = Math.max(1, Math.min(7, int(url.searchParams, "days", 7)));
  const limit = Math.max(3, Math.min(15, int(url.searchParams, "limit", 6)));
  const metric = days <= 1 ? "new_24h_unique" : "new_7d_unique";

  const supabase = supabaseServer();
  const { data, error } = await supabase
    .from("working_student_city_bubbles_v")
    .select("lat,lon,map_city_label,new_24h_unique,new_7d_unique")
    .gt(metric, 0)
    .order(metric, { ascending: false })
    .limit(limit);

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({ rows: data ?? [] });
}
