import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabaseServer";
import { friendlySupabaseError } from "@/lib/supabaseErrors";

export const runtime = "nodejs";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ platform: string; job_id: string }> }
) {
  const { platform, job_id } = await params;
  if (!platform || !job_id) {
    return NextResponse.json({ error: "Missing params" }, { status: 400 });
  }

  const supabase = supabaseServer();
  const { data, error } = await supabase
    .from("working_student_jobs_v")
    .select(
      [
        "platform",
        "job_id",
        "job_url",
        "job_title",
        "company_name",
        "job_location",
        "job_description",
        "posted_at_utc",
        "first_seen_at",
        "last_seen_at",
        "extracted_skills",
      ].join(",")
    )
    .eq("platform", platform)
    .eq("job_id", job_id)
    .maybeSingle();

  if (error) return NextResponse.json({ error: friendlySupabaseError(error.message) }, { status: 500 });
  if (!data) return NextResponse.json({ error: "Not found" }, { status: 404 });
  return NextResponse.json({ job: data });
}
