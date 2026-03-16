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
  const days = int(url.searchParams, "days", 90);
  const daysWindow = days <= 30 ? 30 : 90;
  const limit = Math.max(5, Math.min(25, int(url.searchParams, "limit", 10)));

  const supabase = supabaseServer();
  const { data, error } = await supabase
    .from("working_student_top_skills_v")
    .select("days_window,skill,jobs_unique")
    .eq("days_window", daysWindow)
    .order("jobs_unique", { ascending: false })
    .limit(limit);

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({ days_window: daysWindow, rows: data ?? [] });
}
