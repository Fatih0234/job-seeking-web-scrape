# Scheduling (GitHub Actions)

This repo supports scheduled (cron) crawling via GitHub Actions.

Stepstone implementation details and operator runbook:
- `/Volumes/T7/job-seeking-web-scrape/docs/STEPSTONE_SCRAPING.md`

## Prerequisites

- Add database GitHub Actions secrets used by workflows:
- `SUPABASE_DB_URL` (single connection string), or
- `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DATABASE`, `SUPABASE_USER`, `SUPABASE_PASSWORD` (recommended; assembled in code)
- Optional: `SUPABASE_SSLMODE` (defaults to `require`)
- Format examples are in `/Volumes/T7/job-seeking-web-scrape/.env.example`.

## Workflow

- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/linkedin-crawl.yml`
- Schedule: every 12 hours (UTC)
- Manual runs: use `workflow_dispatch`
  - Optional input `sync_search_definitions`:
    - `true`: runs `scripts/sync_search_definitions.py` first
    - `false`: skips sync (assumes `job_scrape.search_definitions` already exists)

Stepstone has a dedicated workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/stepstone-crawl.yml`
- Schedule: every 12 hours (UTC)
- Manual runs: use `workflow_dispatch`
  - Optional input `sync_search_definitions_stepstone`:
    - `true`: runs `scripts/sync_search_definitions_stepstone.py` first
    - `false`: skips sync and uses existing `job_scrape.stepstone_search_definitions` rows

Stepstone also has a weekly safety-net backfill workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/stepstone-backfill.yml`
- Schedule: weekly (UTC)
- Uses `STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE=7` to widen discovery without re-syncing definitions.

## Discovery-Only Mode

You can skip job detail fetching (which is typically more block-prone) by setting:

- `RUN_DETAILS=0`

This will still run discovery and write deduped `jobs` and `job_search_hits` rows.

## First-Time Setup

1. Run the workflow manually with `sync_search_definitions=true`.
2. Subsequent scheduled runs can use `sync_search_definitions=false` to reduce
   LinkedIn discovery calls.

For Stepstone:
1. Run the workflow manually with `sync_search_definitions_stepstone=true`.
2. Scheduled runs can keep sync enabled (default) unless you manage definitions separately.

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
