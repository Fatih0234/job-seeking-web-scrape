export function friendlySupabaseError(message: string) {
  if (message.includes("Invalid schema: job_scrape")) {
    return (
      "Supabase PostgREST is rejecting schema `job_scrape`.\n" +
      "Fix: Supabase Dashboard -> API -> Exposed schemas -> add `job_scrape`, then retry.\n" +
      "Also ensure SELECT grants exist on the views (see docs/WEB_DASHBOARD.md)."
    );
  }
  return message;
}

