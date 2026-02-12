# Stepstone Scraping (Current State)

This document describes the current Stepstone implementation in this repository, including URL building, discovery logic, detail parsing, dedicated DB schema, deduplication behavior, skill extraction, scheduling, and health checks.

## Scope and Design

- Stepstone uses a dedicated pipeline and dedicated tables under the same Postgres schema `job_scrape`.
- LinkedIn tables and LinkedIn pipeline behavior are not modified by Stepstone ingestion.
- Stepstone requests are Playwright-first for both discovery and details due to intermittent WAF blocks on non-browser requests.

## End-to-End Flow

1. Sync definitions: `configs/stepstone.yaml` -> `job_scrape.stepstone_search_definitions`
2. Discovery crawl: paginate Stepstone result pages and emit only `main` jobs
3. Discovery import: upsert `stepstone_jobs` and insert `stepstone_job_search_hits`
4. Detail crawl: fetch selected jobs and parse normalized detail fields
5. Detail import: upsert `stepstone_job_details` and extract grouped skills
6. Reporting: summarize latest Stepstone crawl status and quality metrics

Primary orchestrator:
- `scripts/run_crawl_stepstone.py`

## Stepstone URL Construction

Implementation:
- `job_scrape/stepstone.py`

Builder:
- `build_search_url(keywords, location, radius=30, sort=2, age_days=None, page=1, action=None, search_origin="Resultlist_top-search", where_type="autosuggest")`

Normalization:
- Lowercase
- Spaces -> `-`
- Keeps digits (zip code patterns like `26121-oldenburg`)
- Keeps Latin letters including umlauts
- Replaces unsupported punctuation with `-`

Path format:
- `/jobs/<keyword-slug>/in-<location-slug>`

Query parameters:
- `radius`: integer, default `30`
- `sort`: `1` (relevance) or `2` (newest), default `2`
- `searchOrigin`: default `Resultlist_top-search`
- `whereType`: default `autosuggest`
- `page`: included only for `page > 1`
- `ag`:
  - when `age_days=1`: `ag=age_1` (last 24h)
  - when `age_days=7`: `ag=age_7` (last 7 days)
- `action`:
  - explicit `action` argument (highest precedence)
  - page 1 + `age_days` -> `facet_selected;age;age_<n>`
  - page > 1 -> `paging_next`
  - page 1 fallback -> `sort_relevance`/`sort_publish`

## Discovery Relevance Model

Stepstone result pages expose counters in DOM attributes:
- `data-resultlist-offers-total`
- `data-resultlist-offers-main`
- `data-resultlist-offers-regional`
- `data-resultlist-offers-semantic`
- and `*-displayed` variants

Current selection rule:
- Parse all `article[id^="job-item-"]` in DOM order.
- Extract numeric `job_id` from `job-item-<id>`.
- Keep only the first `main_displayed` cards.
- Canonical job URL becomes `https://www.stepstone.de/job/<job_id>`.

Markers used for diagnostics only:
- `Noch nichts dabei ...`
- `... außerhalb deiner Region`

These marker texts are logged in page metadata, but filtering is counter-based (`main_displayed`), not marker-based.

## Discovery Spider Behavior

Implementation:
- `job_scrape/spiders/stepstone_discovery_paginated.py`

Input:
- JSON file with `searches[]`, each containing:
  - `search_definition_id`, `search_run_id`, `name`
  - `keywords`, `location_text`
  - `facets` (`radius`, `sort`, `where_type`, `search_origin`)

Output record types:
- `page_fetch`: includes status code, blocked flag, item count, counters, markers
- `job_discovered`: includes `source`, `job_id`, `job_url`, rank, page number

Block detection:
- status in `{403, 429, 503}`
- or body contains fingerprints like `access denied`, `errors.edgesuite.net`, `captcha`

Stop conditions:
- `main_displayed == 0`
- no main items returned
- duplicate-page streak budget reached
- pages/jobs runtime budgets reached
- block circuit breaker reached

Default safe budgets:
- `MAX_PAGES_PER_SEARCH=50`
- `MAX_JOBS_DISCOVERED_PER_SEARCH=2000`
- `DUPLICATE_PAGE_LIMIT=3`
- `CIRCUIT_BREAKER_BLOCKS=3`

## Detail Parsing Behavior

Implementation:
- Spider: `job_scrape/spiders/stepstone_job_detail_batch.py`
- Parser: `job_scrape/stepstone_detail.py`

Extracted fields:
- `job_title`: from `h1`
- `company_name`: `[data-at="metadata-company-name"]`
- `job_location`: `[data-at="metadata-location"]`
- `posted_time_ago`: `[data-at="metadata-online-date"]` with `Erschienen:` prefix removed
- `criteria.contract_type`: `[data-at="metadata-contract-type"]`
- `criteria.work_type`: `[data-at="metadata-work-type"]`
- `job_description`: combined text from `[data-at="job-ad-content"] span.job-ad-display-nfizss`

Description exclusions:
- Salary block text from `[data-at="job-ad-salary"]` is removed.

Derived boolean fields on import:
- `homeoffice_possible` from `work_type` containing `homeoffice` or `home office`
- `full_time` from `work_type` containing `vollzeit` or `full time`
- `part_time` from `work_type` containing `teilzeit` or `part time`

Failure handling:
- Blocked pages are stored with `parse_ok=false`, `last_error='blocked'`
- Missing critical fields (`job_title`) produce parse failure marker
- Debug artifacts for failed detail parses are written to:
  - `output/stepstone_detail_failures/*.html`
  - `output/stepstone_detail_failures/*.png`

## Config and Definition Sync

Config file:
- `configs/stepstone.yaml`

Loader:
- `job_scrape/stepstone_config.py`

Top-level schema:
- `stepstone.searches[]`

Per search fields:
- `name`
- `keywords[]`
- `locations[]`
- optional `country` (default `Germany`)
- optional `sort` (`1/2` or labels like `relevance/newest`, default `2`)
- optional `radius` (default `30`)
- optional `where_type` (default `autosuggest`)
- optional `search_origin` (default `Resultlist_top-search`)
- optional `age_days` (`1` or `7`; omitted means no age filter)

Sync script:
- `scripts/sync_search_definitions_stepstone.py`
- Config path can be overridden with env:
  - `STEPSTONE_CONFIG_PATH=configs/stepstone.backfill.yaml`

Expansion behavior:
- Expands keyword x location combinations into separate DB rows.
- Stable naming pattern:
  - `<base>__loc<idx>_<location_slug>__kw<idx>_<keyword_slug>`

## Dedicated Stepstone Tables

Created by:
- `scripts/create_stepstone_tables.py`

Tables:
- `job_scrape.stepstone_crawl_runs`: crawl-level run tracking
- `job_scrape.stepstone_search_definitions`: synced search definitions
- `job_scrape.stepstone_search_runs`: per-search execution state per crawl
- `job_scrape.stepstone_jobs`: deduped job identities by `job_id`
- `job_scrape.stepstone_job_search_hits`: search-to-job relation with rank/page
- `job_scrape.stepstone_job_details`: parsed details + skill extraction fields

Important behavior:
- Deduped job identity is `stepstone_jobs.job_id` (unique per Stepstone job).
- A single job can appear in many searches; each appearance is preserved in `stepstone_job_search_hits`.
- Stepstone schema is independent of LinkedIn shared tables.

## Skill Extraction Integration

Stepstone details import runs the same deterministic extractor used by LinkedIn:
- Taxonomy: `configs/data-engineering-keyword-taxonomy.yaml`
- Extractor: `job_scrape/skill_extraction.py`
- Importer: `scripts/import_details_stepstone.py`

Stored in `stepstone_job_details`:
- `extracted_skills` (jsonb grouped canonical skills)
- `extracted_skills_version`
- `extracted_skills_extracted_at`

## Runbook

Environment:
- `SUPABASE_DB_URL` must be set.

Prepare tables:
```bash
python -m scripts.create_stepstone_tables
```

Sync definitions:
```bash
python -m scripts.sync_search_definitions_stepstone
```

Sync definitions from custom config path:
```bash
STEPSTONE_CONFIG_PATH=configs/stepstone.backfill.yaml python -m scripts.sync_search_definitions_stepstone
```

Run full Stepstone crawl:
```bash
python -m scripts.run_crawl_stepstone
```

Smoke run with limited detail volume:
```bash
MAX_JOB_DETAILS_PER_RUN=5 python -m scripts.run_crawl_stepstone
```

Discovery-only run:
```bash
RUN_DETAILS=0 python -m scripts.run_crawl_stepstone
```

Report latest Stepstone run:
```bash
REPORT_SOURCE=stepstone python -m scripts.report_latest_run
```

## Scheduling (GitHub Actions)

Workflow:
- `.github/workflows/stepstone-crawl.yml`

Schedule:
- Every 12 hours (UTC): `0 */12 * * *`

Workflow behavior:
- Installs dependencies and Playwright Chromium
- Runs unit tests
- Runs Stepstone orchestrator
- Runs latest-run report
- Uploads `output/**` artifacts

Manual dispatch input:
- `sync_search_definitions_stepstone` (`true`/`false`)

## Health Monitoring Queries

Latest crawl run:
```sql
select id, started_at, finished_at, trigger, status, error, stats
from job_scrape.stepstone_crawl_runs
order by started_at desc
limit 1;
```

Latest per-search statuses:
```sql
select status, count(*) as n
from job_scrape.stepstone_search_runs
where crawl_run_id = (
  select id from job_scrape.stepstone_crawl_runs order by started_at desc limit 1
)
group by status
order by status;
```

Duplicate pressure across keyword variants:
```sql
select
  count(*) as hits,
  count(distinct h.job_id) as unique_jobs,
  count(*) - count(distinct h.job_id) as duplicate_hits
from job_scrape.stepstone_job_search_hits h
join job_scrape.stepstone_search_runs sr on sr.id = h.search_run_id
where sr.crawl_run_id = (
  select id from job_scrape.stepstone_crawl_runs order by started_at desc limit 1
);
```

Detail parse quality:
```sql
select
  count(*) as total,
  count(*) filter (where parse_ok) as parse_ok,
  count(*) filter (where last_error='blocked') as blocked
from job_scrape.stepstone_job_details;
```

Skill extraction coverage:
```sql
select
  count(*) filter (where parse_ok) as parse_ok_total,
  count(*) filter (where parse_ok and extracted_skills is not null) as parse_ok_with_skills
from job_scrape.stepstone_job_details;
```

## Known Limits

- Stepstone anti-bot responses can still occur; circuit breakers reduce repeated blocking.
- DOM selectors and result counters may change if Stepstone updates markup.
- `posted_time_ago` is stored as source text (not converted to absolute datetime).
- Full relevance logic currently uses Stepstone counters (`main_displayed`) and does not score semantic quality itself.
