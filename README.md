# job-seeking-web-scrape

Scrape job posting sites using **Scrapy + Playwright** (via `scrapy-playwright`).

Current focus: **LinkedIn (guest/public jobs pages)**.

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

## Notes / Limitations
- LinkedIn may rate-limit or block automation. We keep the system conservative, and we save debug artifacts (HTML + screenshots) under `output/` when needed.
- "Every city in a country" is achieved by **omitting** `f_PP` (no city filter) while setting `geoId` to the country.

## Documentation
- Project handoff / history / decisions: `docs/AI_HANDOFF.md`

