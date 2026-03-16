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
  const months = Math.max(3, Math.min(12, int(url.searchParams, "months", 6)));

  const supabase = supabaseServer();
  const { data, error } = await supabase
    .from("working_student_trend_v")
    .select("bucket_start,jobs_unique")
    .order("bucket_start", { ascending: true });

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });

  const rows = (data ?? []).slice(-months);
  return NextResponse.json({ rows });
}
