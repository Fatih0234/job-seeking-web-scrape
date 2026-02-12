# job-seeking-web-scrape

Scrape job posting sites using **Scrapy + Playwright** (via `scrapy-playwright`).

Current focus:
- **LinkedIn** (guest/public jobs pages)
- **Stepstone** (Playwright-first list + detail scraping)
- **XING** (Playwright-first discovery with show-more pagination)

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
- `job_scrape/stepstone.py` (URL building + counters + list parsing)
- `job_scrape/stepstone_detail.py` (detail HTML parsing)
- `job_scrape/spiders/stepstone_discovery_paginated.py`
- `job_scrape/spiders/stepstone_job_detail_batch.py`
- `scripts/sync_search_definitions_stepstone.py`
- `scripts/run_discovery_stepstone.py`
- `scripts/run_details_stepstone.py`
- `scripts/run_crawl_stepstone.py`
- `scripts/create_stepstone_tables.py`

### 5) XING discovery (DB pipeline, v1)
- YAML-driven XING searches (keywords, optional locations, optional `cityId`) synced to `search_definitions`.
- Discovery is Playwright-first and paginates by repeatedly clicking **Show more**.
- This milestone is discovery-only (no XING detail scraper yet).

Code:
- `configs/xing.yaml` (example config)
- `job_scrape/xing_config.py` (config loader)
- `job_scrape/xing.py` (URL building + list parsing helpers)
- `job_scrape/spiders/xing_discovery_paginated.py`
- `scripts/sync_search_definitions_xing.py`
- `scripts/run_discovery_xing.py`
- `scripts/run_crawl_xing.py`

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

### Stepstone: discovery + details orchestrated run
```bash
source .venv/bin/activate
python -m scripts.run_crawl_stepstone
```

### Stepstone: report latest run
```bash
source .venv/bin/activate
REPORT_SOURCE=stepstone python -m scripts.report_latest_run
```

### XING: sync search definitions
```bash
source .venv/bin/activate
python -m scripts.sync_search_definitions_xing
```

### XING: discovery-only orchestrated run
```bash
source .venv/bin/activate
python -m scripts.run_crawl_xing
```

### XING: report latest run
```bash
source .venv/bin/activate
REPORT_SOURCE=xing python -m scripts.report_latest_run
```

## Notes / Limitations
- LinkedIn may rate-limit or block automation. We keep the system conservative, and we save debug artifacts (HTML + screenshots) under `output/` when needed.
- "Every city in a country" is achieved by **omitting** `f_PP` (no city filter) while setting `geoId` to the country.
- Stepstone may return `403 Access Denied` on non-browser requests; the Stepstone pipeline uses Playwright-first requests for both discovery and detail pages.
- XING search URLs do not need `id`; the site auto-generates it on load.
- XING `cityId` is optional; keywords-only and location-text-only queries are valid.

## Documentation
- Project handoff / history / decisions: `docs/AI_HANDOFF.md`
- Database ERD + read/write flows: `docs/DB_ERD_AND_FLOWS.md`
- Stepstone implementation and runbook: `docs/STEPSTONE_SCRAPING.md`
