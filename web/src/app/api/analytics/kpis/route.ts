import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabaseServer";
import { friendlySupabaseError } from "@/lib/supabaseErrors";

export const runtime = "nodejs";

export async function GET() {
  const supabase = supabaseServer();
  const { data, error } = await supabase
    .from("working_student_kpis_v")
    .select("total_active_jobs,remote_roles,new_companies_7d,as_of")
    .maybeSingle();

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  return NextResponse.json({ kpis: data });
}
