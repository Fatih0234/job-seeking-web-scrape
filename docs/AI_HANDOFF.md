# AI Handoff: job-seeking-web-scrape (LinkedIn Phase)

This document is a single handout for the next AI session to take over the project.
It captures what was built, why it was built that way, and where the sharp edges are.

## 1) Project Goal (Current Phase)

We are building a job scraping system using:
- Scrapy as the crawler framework (queues, throttling, output feeds)
- Playwright for rendering when the target site requires JavaScript

The first target site is LinkedIn. The goal was to start with a small, repeatable milestone:
1. Scrape the first page of a LinkedIn guest jobs search (job cards list).
2. From that list, pick the first job and extract structured details from the job detail HTML.
3. Make searches configurable and dynamic (no hardcoded geo ids or filter codes).

## 2) Key Observations About LinkedIn (Guest Jobs)

### 2.1 URLs and identifiers

LinkedIn job posting identifiers are numeric. Job URLs appear in a few patterns:
- `/jobs/view/<id>/`
- `/jobs/view/<slug>-<id>`

This mattered because early extraction logic assumed only `/jobs/view/<id>` and missed slug-based URLs.
We updated extraction to handle both patterns.

### 2.2 Country geoId vs city filter f_PP

LinkedIn uses multiple geo-related query parameters:
- `geoId=<id>`: the *selected country/region* from the main search bar location.
- `f_PP=<id>`: an *optional location facet filter* (populated place / region filter).

Important decision:
- For "every city within a country", do not enumerate cities.
- Instead, omit `f_PP` entirely. With `geoId=Germany`, results include jobs from Berlin, Munich, etc.

### 2.3 Filter facets are discoverable from HTML

The LinkedIn guest search page includes filter UI elements as HTML inputs:
- Job type: `f_JT` with values like `F`, `P`, `C`, ...
- Experience: `f_E` values like `1..5`
- Remote: `f_WT` values like `1`, `2`, `3`
- Date posted: `f_TPR` values like `r86400`, `r604800`, etc (empty = Any time)

Instead of hardcoding these values, we parse the search page HTML to discover the mapping
from human labels to codes.

### 2.4 Localization exists on job detail pages

On some regional domains (e.g. `de.linkedin.com`), the criteria labels on job detail pages
may be translated (German examples observed):
- "Karrierestufe" (Seniority level)
- "Beschäftigungsverhältnis" (Employment type)
- "Tätigkeitsbereich" (Job function)
- "Branchen" (Industries)

We added a small label mapping fallback so the criteria extraction still works.

## 3) Architecture Overview

### 3.1 Parsing code (pure HTML -> structured dicts)

Pure parsing modules are intentionally separated from spiders.
This allows:
- unit tests on fixtures without network access
- easy reuse in other spiders/pipelines

Files:
- `job_scrape/linkedin.py`
  - `parse_search_results(html, search_url=...)`
  - `extract_job_id(href, entity_urn=...)` handles `/jobs/view/<id>` and `/jobs/view/<slug>-<id>`
  - `canonicalize_job_url()` strips tracking query params

- `job_scrape/linkedin_detail.py`
  - `parse_job_detail(html)` extracts:
    - title, company, location, posted_time_ago
    - job_description (flattened text from `.description__text--rich`)
    - criteria list (label/value pairs from `.description__job-criteria-list`)

### 3.2 Dynamic configuration and discovery (YAML + caching)

Files:
- `configs/linkedin.yaml`
  - Example searches: keywords + countries + optional cities list + filters by label

- `job_scrape/yaml_config.py`
  - Loads YAML into dataclasses (`LinkedInConfig`, `LinkedInSearchSpec`, ...)
  - Validates basic schema

- `job_scrape/linkedin_typeahead.py`
  - Uses LinkedIn guest typeahead endpoint:
    - `https://www.linkedin.com/jobs-guest/api/typeaheadHits`
  - Resolves:
    - `COUNTRY_REGION` for country `geoId`
    - `POPULATED_PLACE` for city facet `f_PP` if requested
  - Caches results in `cache/linkedin_geo.json`

- `job_scrape/linkedin_facets.py`
  - Parses search page HTML to discover available filter options and their codes
  - Builds label->value map (case-insensitive)
  - Resolves requested human labels (or accepts raw codes)
  - Caches maps in `cache/linkedin_facets.json`

### 3.3 Spiders (network + scheduling)

Spiders:
- `linkedin_jobs_search`
  - Playwright-backed list-page scrape (good for understanding behavior and debugging)

- `linkedin_yaml_search`
  - Reads YAML and runs the configured searches
  - Resolves geo ids via typeahead if not provided
  - Discovers facet codes by parsing HTML
  - Generates final search URLs and yields list items

- `linkedin_first_job_detail`
  - End-to-end sanity check:
    1. Fetch search results (Germany example)
    2. Take first job card
    3. Visit job URL (via Playwright)
    4. Extract job detail fields (including description + criteria)
  - Saves `output/linkedin_first_job_detail.html` and `output/linkedin_first_job_detail.png`
    to debug selector drift or blocks.

## 4) Why We Made These Decisions

### 4.1 Avoid hardcoding LinkedIn ids/codes
Goal: let users specify:
- countries by name ("Germany")
- optional city list ("Berlin")
- filters by human labels ("Full-time", "Remote", "Past week")

So the system can adapt to:
- different countries without manual `geoId` lookup
- LinkedIn changing numeric codes (less likely, but possible)
- localized labels on job detail pages

### 4.2 Caching
LinkedIn is sensitive to automation. Reducing repeated "discovery calls" helps:
- fewer requests
- fewer opportunities to trigger rate limiting
- faster repeat runs

Cache files are in `cache/` and gitignored. They can be deleted safely.

### 4.3 Keep parsing functions pure
Parsing modules do not call the network and do not depend on Scrapy state.
This makes unit tests stable and keeps logic reusable.

## 5) Testing Strategy

Unit tests:
- `tests/test_linkedin_extract.py` (search list parsing + job id/url logic)
- `tests/test_linkedin_facets.py` (facet mapping from HTML)
- `tests/test_linkedin_detail_extract.py` (detail parsing: title/company/location/posted + criteria + description)

Fixtures:
- `tests/fixtures/linkedin_search_snippet.html`
- `tests/fixtures/linkedin_job_detail_snippet.html`

End-to-end smoke test:
- `scrapy crawl linkedin_first_job_detail -O output/linkedin_first_job_detail.jsonl`

## 6) Known Sharp Edges / Next Steps

1) Anti-bot and inconsistent responses
- LinkedIn can serve different HTML by region, time, and bot heuristics.
- When extraction fails, inspect `output/linkedin_first_job_detail.html` and `.png`.

2) Multi-language criteria mapping
- We added a small German mapping.
- If you crawl other locales, extend label mapping in `job_scrape/linkedin_detail.py`.

3) Structured description output
Currently `job_description` is plain text with whitespace collapsed.
If you want richer output (HTML, bullet preservation), introduce:
- `job_description_html` (raw HTML)
- `job_description_text` (plain text)
and choose a canonical representation.

4) Pagination + scaling
Right now we focus on page 1.
Scaling should:
- paginate via `start=<offset>` or equivalent
- add queueing strategy (per country, per keyword, per facet)
- add backoff and retry logic for blocks

5) Storage model
Currently output is feed export (JSONL).
If you need dedupe and incremental updates, add a persistent store:
- SQLite/Postgres + unique key on `source + job_id`

## 7) Quick Commands

Setup:
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m playwright install chromium
```

Run YAML:
```bash
scrapy crawl linkedin_yaml_search -a config=configs/linkedin.yaml -O output/linkedin_yaml.jsonl
```

Smoke test detail parse:
```bash
scrapy crawl linkedin_first_job_detail -O output/linkedin_first_job_detail.jsonl
```

