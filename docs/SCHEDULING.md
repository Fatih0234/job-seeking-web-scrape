# Scheduling (GitHub Actions)

This repo supports scheduled (cron) crawling via GitHub Actions.

Stepstone implementation details and operator runbook:
- `/Volumes/T7/job-seeking-web-scrape/docs/STEPSTONE_SCRAPING.md`

## Prerequisites

- Add the database GitHub Actions secrets used by workflows:
  - `SUPABASE_DB_URL` (single connection string), or
  - `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DATABASE`, `SUPABASE_USER`, `SUPABASE_PASSWORD` (recommended; assembled in code)
  - Optional: `SUPABASE_SSLMODE` (defaults to `require`)
- For map geocoding workflow, also add:
  - `GEOAPIFY_API_KEY`
- Format examples are in `/Volumes/T7/job-seeking-web-scrape/.env.example`.

## Workflow

- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/linkedin-crawl.yml`
- Schedule: daily at `01:15 UTC`
- Manual runs: use `workflow_dispatch`
  - Optional input `sync_search_definitions`:
    - `true`: runs `scripts/sync_search_definitions.py` first
    - `false`: skips sync (assumes `job_scrape.search_definitions` already exists)
- On success, refreshes dashboard read models via `scripts.refresh_dashboard_read_models`

Stepstone has a dedicated workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/stepstone-crawl.yml`
- Schedule: daily at `03:30 UTC`
- Manual runs: use `workflow_dispatch`
  - Optional input `sync_search_definitions_stepstone`:
    - `true`: runs `scripts/sync_search_definitions_stepstone.py` first
    - `false`: skips sync and uses existing `job_scrape.stepstone_search_definitions` rows
- On success, refreshes dashboard read models via `scripts.refresh_dashboard_read_models`

Stepstone also has a weekly safety-net backfill workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/stepstone-backfill.yml`
- Schedule: weekly (UTC)
- Uses `STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE=7` to widen discovery without re-syncing definitions.
XING has a dedicated workflow (incremental, last 24 hours):
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/xing-crawl-last24h.yml`
- Schedule: daily at `05:45 UTC`
- Incremental filter: `sincePeriod=LAST_24_HOURS`
  - Implemented via env var: `XING_SINCE_PERIOD=LAST_24_HOURS`
  - This applies to discovery URL construction (no DB definition change required).
- Safety budgets (workflow defaults):
  - `MAX_PAGES_PER_SEARCH=20`
  - `MAX_JOBS_DISCOVERED_PER_SEARCH=800`
  - `MAX_JOB_DETAILS_PER_RUN=100`
  - `XING_DISCOVERY_TIMEOUT_SECONDS=4200`
  - `XING_DETAILS_TIMEOUT_SECONDS=1800`
- Notes:
  - XING job detail 410s are treated as expected churn and are marked inactive to avoid endless retries.
  - Integrity is strictly gated by `scripts.verify_xing_workflow_run` (run-level DB reconciliation + stale auto-heal).
  - On success, refreshes dashboard read models via `scripts.refresh_dashboard_read_models`.

LinkedIn details has a dedicated workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/linkedin-details.yml`
- Schedule: twice daily at `11:15 UTC` and `23:15 UTC`
- Behavior:
  - runs `RUN_DISCOVERY=0`, `RUN_DETAILS=1`
  - keeps detail scraping conservative to reduce blocks
  - refreshes dashboard read models on success

XING details catch-up has a dedicated workflow (details-only):
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/xing-details-catchup.yml`
- Schedule: daily at `08:30 UTC`
- Behavior:
  - runs `RUN_DISCOVERY=0`, `RUN_DETAILS=1` (no additional discovery load)
  - `MAX_JOB_DETAILS_PER_RUN=100`
  - `DETAIL_LAST_SEEN_WINDOW_DAYS=7`
  - `XING_DETAILS_TIMEOUT_SECONDS=2400`
  - uses `concurrency.cancel-in-progress=false` to avoid dropping in-flight state
  - integrity is strictly gated by `scripts.verify_xing_workflow_run`
  - refreshes dashboard read models on success

## XING Integrity Verification

Both XING workflows run these protections on every execution:
- `scripts.report_latest_run` with `REPORT_RUN_ID` scoped to the current `crawl_run_id`
- `scripts.verify_xing_workflow_run --strict 1 --repair-stale 1`

Verifier behavior:
- auto-heals stale `job_scrape.xing_crawl_runs` rows (`status='running'` older than 180 minutes) and their child `xing_search_runs`
- re-checks stale state after repair
- fails the workflow if integrity checks still fail

Manual 14-day diagnostics:

```bash
python -m scripts.xing_cron_diagnostics --days 14 --strict 1
```

For failure classes and remediation steps, see:
- `/Volumes/T7/job-seeking-web-scrape/docs/xing_cron_reconciliation.md`

Lifecycle maintenance has a dedicated workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/job-lifecycle-maintenance.yml`
- Schedule: daily at `02:30 UTC`
- Manual runs: use `workflow_dispatch`
  - Optional input `dry_run`:
    - `true`: computes counts only (no updates/deletes)
    - `false`: applies soft-expire + hard-delete operations
- On success, refreshes dashboard read models via `scripts.refresh_dashboard_read_models`

Geocode enrichment has a dedicated workflow:
- Workflow file: `/Volumes/T7/job-seeking-web-scrape/.github/workflows/geocode-enrichment.yml`
- Triggers:
  - daily at `12:50 UTC`
  - manual `workflow_dispatch`
- Behavior:
  - refreshes dashboard read models on success
  - only calls Geoapify when preflight detects missing cache keys or retryable due rows
  - keeps geocoding isolated from scraper workflows

## Discovery-Only Mode

You can skip job detail fetching (which is typically more block-prone) by setting:

- `RUN_DETAILS=0`

This will still run discovery and write deduped `jobs` and `job_search_hits` rows.

## Details Selection Freshness Guard

All details runners now ignore likely-dead jobs by default:

- `DETAIL_LAST_SEEN_WINDOW_DAYS=60`

Applies to:
- `scripts/run_details.py`
- `scripts/run_details_stepstone.py`
- `scripts/run_details_xing.py`

Selection requires:
- `jobs.last_seen_at > now() - interval '60 days'` (or your override)

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
- DB dedupe on `(source, job_id)` makes reruns and overlapping fetch windows safe.

## Lifecycle Maintenance Env Vars

`scripts/maintain_job_lifecycle.py` supports:
- `LIFECYCLE_STALE_AFTER_DAYS` (default `60`)
- `LIFECYCLE_HARD_DELETE_AFTER_DAYS` (default `120`)
- `LIFECYCLE_MAX_CRAWL_AGE_HOURS` (default `36`)
- `LIFECYCLE_DRY_RUN` (`0/1`, default `0`)
- `LIFECYCLE_TRIGGER` (`github_schedule`, `github_manual`, `manual`, etc.)

## Geocode Operations

Workflow runtime defaults (in `geocode-enrichment.yml`):
- `GEOCODE_ENSURE_SCHEMA=0`
- `GEOCODE_SEED_CACHE=1`
- `GEOCODE_MAX_LOCATIONS_PER_RUN=300`
- `GEOAPIFY_BATCH_SIZE=100`
- `GEOAPIFY_POLL_TIMEOUT_SECONDS=120`
- `GEOCODE_MAX_ATTEMPTS=6`
- `GEOCODE_RETRY_BASE_MINUTES=60`
- `GEOCODE_RETRY_MAX_MINUTES=10080`

`failed_permanent` policy is manual reset only. Use targeted reset SQL:

```sql
update job_scrape.location_geocode_cache
set
  status = 'pending',
  attempt_count = 0,
  last_error = null,
  next_retry_at = now(),
  updated_at = now()
where provider = 'geoapify'
  and status = 'failed_permanent'
  and location_text_norm in ('oldenburg');
```

Then run `Geocode Enrichment` manually via `workflow_dispatch`.

Coverage inspection queries:

```sql
select status, count(*) from job_scrape.location_geocode_cache group by status order by status;
```

```sql
select
  platform,
  count(*) as jobs_total,
  count(*) filter (where is_geocoded) as geocoded_jobs
from job_scrape.jobs_dashboard_map_v
group by platform
order by platform;
```
