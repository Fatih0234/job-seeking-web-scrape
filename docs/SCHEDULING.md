# Scheduling (GitHub Actions)

This repo supports scheduled (cron) crawling via GitHub Actions.

## Prerequisites

- Add a GitHub Actions secret named `SUPABASE_DB_URL`.
  - Format example is in `/Volumes/T7/job-seeking-web-scrape/.env.example`.

## Workflow

- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/linkedin-crawl.yml`
- Schedule: every 12 hours (UTC)
- Manual runs: use `workflow_dispatch`
  - Optional input `sync_search_definitions`:
    - `true`: runs `scripts/sync_search_definitions.py` first
    - `false`: skips sync (assumes `job_scrape.search_definitions` already exists)

## First-Time Setup

1. Run the workflow manually with `sync_search_definitions=true`.
2. Subsequent scheduled runs can use `sync_search_definitions=false` to reduce
   LinkedIn discovery calls.

## `f_TPR` Incremental Discovery Policy

Discovery runs automatically adjust `f_TPR` *only when the stored search definition
is effectively "Any time"* (i.e., `f_TPR` is missing or empty).

Defaults (overridable via env vars in the workflow):
- `DISCOVERY_TPR_POLICY=auto_if_any_time`
- `DISCOVERY_TPR_RECENT_HOURS=30`
- `DISCOVERY_TPR_RECENT_CODE=r86400` (Past 24 hours)
- `DISCOVERY_TPR_FALLBACK_CODE=r604800` (Past week)

Behavior:
- If there is **no finished history** for a search definition: omit `f_TPR` (backfill / Any time).
- If the last successful unblocked run is **recent** (within `DISCOVERY_TPR_RECENT_HOURS`): use `r86400`.
- Otherwise: use `r604800`.

Note:
- Overlap is intentional (e.g. a 12h schedule with a 24h window). DB dedupe on
  `(source, job_id)` makes overlaps safe.

