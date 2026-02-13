# job-seeking-web-scrape

Scrape job posting sites using **Scrapy + Playwright** (via `scrapy-playwright`).

Current focus:
- **LinkedIn** (guest/public jobs pages)
- **Stepstone** (Playwright-first list + detail scraping)
- **XING** (fully isolated DB pipeline: discovery + details)

## What Exists Today

### 1) LinkedIn search (list page) extraction
- Parses job cards from the LinkedIn guest jobs search results page.
- Extracts `job_id`, `job_url`, `title`, `company`, `location`, `posted_at`, `rank`.

Code:
- `job_scrape/linkedin.py` (HTML parsing + job id/url normalization)
- `job_scrape/spiders/linkedin_jobs_search.py` (Playwright-backed list scrape)

### 2) YAML-driven LinkedIn searches (dynamic geo + dynamic filters)
- User config is YAML: countries by name, optional city list, filters by human label.
- Dynamically resolves:
  - country `geoId` via LinkedIn guest typeahead API
  - optional city `f_PP` via the same typeahead API
  - filter codes (`f_JT`, `f_E`, `f_WT`, `f_TPR`) by parsing LinkedIn search HTML
- Uses a local JSON cache under `cache/` to reduce repeated calls.

Code:
- `configs/linkedin.yaml` (example config)
- `job_scrape/yaml_config.py` (config loader)
- `job_scrape/linkedin_typeahead.py` (typeahead + cache helpers)
- `job_scrape/linkedin_facets.py` (discover filter values/labels from HTML)
- `job_scrape/spiders/linkedin_yaml_search.py` (runs the config)

### 3) LinkedIn job detail extraction (one job detail page)
- Fetches the **first job** from the Germany example search results page.
- Visits its detail page and extracts:
  - `job_title`, `company_name`, `job_location`, `posted_time_ago`, `job_description`
  - criteria block: `seniority_level`, `employment_type`, `job_function`, `industries`

Code:
- `job_scrape/linkedin_detail.py` (detail HTML parsing)
- `job_scrape/spiders/linkedin_first_job_detail.py` (end-to-end check spider)

### 4) Stepstone discovery + details (DB pipeline)
- YAML-driven Stepstone searches (keyword x location combinations) synced to `stepstone_search_definitions`.
- Paginated discovery keeps only **main** jobs using `data-resultlist-offers-main-displayed`.
- Detail scraping is Playwright-first to reduce WAF block failures.

Code:
- `configs/stepstone.yaml` (example config)
- `configs/stepstone.backfill.yaml` (one-time backfill profile)
- `job_scrape/stepstone.py` (URL building + counters + list parsing)
- `job_scrape/stepstone_detail.py` (detail HTML parsing)
- `job_scrape/spiders/stepstone_discovery_paginated.py`
- `job_scrape/spiders/stepstone_job_detail_batch.py`
- `scripts/sync_search_definitions_stepstone.py`
- `scripts/run_discovery_stepstone.py`
- `scripts/run_details_stepstone.py`
- `scripts/run_crawl_stepstone.py`
- `scripts/create_stepstone_tables.py`

### 5) XING discovery + details (isolated DB pipeline, v2)
- YAML-driven XING searches (keywords, optional locations, optional `cityId`) synced to `xing_search_definitions`.
- Each keyword expands to its own search definition/run; keyword duplicates are removed case-insensitively while preserving first order.
- Discovery is Playwright-first and paginates by repeatedly clicking **Show more**.
- Raw appearances are kept in `xing_job_search_hits`; deduped master jobs are kept in `xing_jobs`.
- Details are Playwright-first with JSON-LD-first extraction for stable fields.
- Skill extraction uses `configs/data-engineering-keyword-taxonomy.yaml` when `job_description` is available.
- External job ads are stored from list-card metadata only (no description crawl).

Code:
- `configs/xing.yaml` (example config)
- `job_scrape/xing_config.py` (config loader)
- `job_scrape/xing.py` (URL building + list parsing helpers)
- `job_scrape/xing_detail.py` (detail HTML parsing)
- `job_scrape/spiders/xing_discovery_paginated.py`
- `job_scrape/spiders/xing_job_detail_batch.py`
- `scripts/create_xing_tables.py`
- `scripts/xing_crawl_common.py`
- `scripts/sync_search_definitions_xing.py`
- `scripts/import_discovery_xing.py`
- `scripts/import_details_xing.py`
- `scripts/run_discovery_xing.py`
- `scripts/run_details_xing.py`
- `scripts/run_crawl_xing.py`
- `scripts/backfill_xing_from_shared.py`

### 6) Unified dashboard view (LinkedIn + Stepstone + XING)
- Creates `job_scrape.jobs_dashboard_v` for cross-platform dashboard reads.
- Includes shared fields plus normalized:
  - `posted_at_utc`
  - `posted_at_source`
  - `posted_at_parse_ok`
  - `posted_at_parse_detail`
- Uses `(platform, job_id)` as the dataset key.

Code:
- `scripts/create_dashboard_view.py`

### 7) Job lifecycle maintenance (stale + expiry)
- Jobs are tracked with freshness fields:
  - discovery timestamps: `first_seen_at`, `last_seen_at`
  - lifecycle state: `is_active`, `stale_since_at`, `expired_at`, `expire_reason`
- Details crawlers skip likely-dead jobs using:
  - `DETAIL_LAST_SEEN_WINDOW_DAYS` (default `60`)
- Daily lifecycle maintenance can:
  - soft-expire jobs unseen for `60` days
  - hard-delete jobs unseen for `120` days (safe child-first delete order)

Code:
- `scripts/ensure_lifecycle_schema.py`
- `scripts/maintain_job_lifecycle.py`

### 8) Map geocoding layer (Geoapify + cache + map view)
- Adds persistent geocoding cache table: `job_scrape.location_geocode_cache`
- Seeds from distinct `job_location` values in `job_scrape.jobs_dashboard_v`
- Country scope policy:
  - LinkedIn + Stepstone: `de`
  - XING: `de,at,ch`
- Creates map-ready view: `job_scrape.jobs_dashboard_map_v`
  - includes `lat`, `lon`, geocode status/quality metadata

Code:
- `scripts/ensure_geocode_schema.py`
- `scripts/geocode_locations_geoapify.py`
- `scripts/create_dashboard_map_view.py`


## Setup

From `/Volumes/T7/job-seeking-web-scrape`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Run

### List page (fixed Germany example)
```bash
source .venv/bin/activate
scrapy crawl linkedin_jobs_search -O output/linkedin_jobs_germany_data_eng.jsonl
```

### YAML config runner
```bash
source .venv/bin/activate
scrapy crawl linkedin_yaml_search -a config=configs/linkedin.yaml -O output/linkedin_yaml.jsonl
```

### First job detail (smoke test)
```bash
source .venv/bin/activate
scrapy crawl linkedin_first_job_detail -O output/linkedin_first_job_detail.jsonl
```

### Stepstone: sync search definitions
```bash
source .venv/bin/activate
python -m scripts.sync_search_definitions_stepstone
```

### Stepstone: sync with custom config (example backfill profile)
```bash
source .venv/bin/activate
STEPSTONE_CONFIG_PATH=configs/stepstone.backfill.yaml python -m scripts.sync_search_definitions_stepstone
```

### Stepstone: discovery + details orchestrated run
```bash
source .venv/bin/activate
python -m scripts.run_crawl_stepstone
```

### Stepstone: full details catch-up (watchdog + auto-restart batches)
```bash
source .venv/bin/activate
python -m scripts.run_stepstone_details_catchup
```

### Stepstone: report latest run
```bash
source .venv/bin/activate
REPORT_SOURCE=stepstone python -m scripts.report_latest_run
```

### XING: create isolated tables
```bash
source .venv/bin/activate
python -m scripts.create_xing_tables
```

### XING: one-time backfill from shared tables
```bash
source .venv/bin/activate
python -m scripts.backfill_xing_from_shared
```

### XING: sync search definitions
```bash
source .venv/bin/activate
python -m scripts.sync_search_definitions_xing
```

### XING: discovery + details orchestrated run
```bash
source .venv/bin/activate
python -m scripts.run_crawl_xing
```

### XING: details-only run (requires `CRAWL_RUN_ID`)
```bash
source .venv/bin/activate
CRAWL_RUN_ID=<existing_xing_crawl_run_id> python -m scripts.run_details_xing
```

### XING: report latest run
```bash
source .venv/bin/activate
REPORT_SOURCE=xing python -m scripts.report_latest_run
```
Includes `hits_total`, `unique_jobs_total`, and `duplicates_removed_total` for the latest crawl.

### Dashboard: create/update unified view
```bash
source .venv/bin/activate
python -m scripts.create_dashboard_view
```

### Dashboard: monitor unparsed posted-time formats
```bash
source .venv/bin/activate
python -m scripts.report_posted_time_parse_gaps
```

### Map: ensure geocode cache schema
```bash
source .venv/bin/activate
python -m scripts.ensure_geocode_schema
```

### Map: geocode distinct locations (Geoapify batch)
```bash
source .venv/bin/activate
python -m scripts.geocode_locations_geoapify
```

### Map: create/update enriched map view
```bash
source .venv/bin/activate
python -m scripts.create_dashboard_map_view
```

### Map: event-driven geocode orchestration (GitHub Actions)
- Workflow: `.github/workflows/geocode-enrichment.yml`
- Triggered after successful:
  - `LinkedIn Crawl`
  - `LinkedIn Details`
  - `Stepstone Crawl`
- Also runs as daily safety net at `03:50 UTC` and via manual `workflow_dispatch`.
- Requires `GEOAPIFY_API_KEY` as a GitHub Actions secret.

### Map: manual reset for failed-permanent geocodes
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
After reset, rerun `Geocode Enrichment` via Actions `workflow_dispatch`.

### Lifecycle: ensure schema
```bash
source .venv/bin/activate
python -m scripts.ensure_lifecycle_schema
```

### Lifecycle: dry run (no mutations)
```bash
source .venv/bin/activate
LIFECYCLE_DRY_RUN=1 python -m scripts.maintain_job_lifecycle
```

### Lifecycle: apply maintenance
```bash
source .venv/bin/activate
python -m scripts.maintain_job_lifecycle
```


## Notes / Limitations
- LinkedIn may rate-limit or block automation. We keep the system conservative, and we save debug artifacts (HTML + screenshots) under `output/` when needed.
- "Every city in a country" is achieved by **omitting** `f_PP` (no city filter) while setting `geoId` to the country.
- Stepstone may return `403 Access Denied` on non-browser requests; the Stepstone pipeline uses Playwright-first requests for both discovery and detail pages.
- XING search URLs do not need `id`; the site auto-generates it on load.
- XING `cityId` is optional; keywords-only and location-text-only queries are valid.
- XING dedupe is cross-keyword and cross-search-run in `xing_jobs` (`job_id` key). Raw hits remain in `xing_job_search_hits`.
- XING external job ads (`External job ad`) may link outside XING; we store list-visible fields for these and skip description crawling.
- Because external list-only ads typically have no description page, `extracted_skills` can be null for those rows.
- Geoapify batch geocoding is asynchronous; some jobs can stay `pending` briefly and get retried by backoff.

## Documentation
- Project handoff / history / decisions: `docs/AI_HANDOFF.md`
- Database ERD + read/write flows: `docs/DB_ERD_AND_FLOWS.md`
- Stepstone implementation and runbook: `docs/STEPSTONE_SCRAPING.md`
