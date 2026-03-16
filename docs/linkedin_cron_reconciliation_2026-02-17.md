# LinkedIn Cron Reconciliation (2026-02-17 UTC)

## Scope
- Platform: LinkedIn shared tables (`job_scrape.crawl_runs`, `job_scrape.search_runs`)
- Intent: One-time operational repair for stale/inconsistent `running` states.

## Applied Fix
- Mark stale crawl runs (`status='running'` older than 180 minutes) as `abandoned`.
- Mark their in-flight search runs as `failed`.
- Mark orphan running search runs as `failed` when parent crawl run is already non-running.

## SQL Outcome
- `stale_runs_found`: `1`
- `crawl_runs_marked_abandoned`: `1`
- `search_runs_failed_from_stale`: `33`
- `orphan_search_runs_failed`: `74`

### Crawl Run IDs Marked `abandoned`
- `4a11abfc-385b-4afc-96c8-623ae179adc7`

### Crawl Run IDs With Orphan Search Runs Fixed
- `77b19b40-3708-4313-ab54-bfe275c7ddf4`
- `cdf5d59f-9bdb-4f10-93ac-47defd1ceb1c`
- `e2ee8b3f-7f29-4f18-ab6a-a0e4b80b4306`

## Post-Fix Acceptance Checks
- `stale_running_crawl_runs` (>3h): `0`
- `inconsistent_running_search_runs`: `0`
