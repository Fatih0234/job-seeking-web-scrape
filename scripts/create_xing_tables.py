from __future__ import annotations

from scripts.db import connect
from scripts.ensure_lifecycle_schema import ensure_schema


SQL = """
create schema if not exists job_scrape;

create table if not exists job_scrape.xing_crawl_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  trigger text not null,
  status text not null default 'running',
  stats jsonb not null default '{}'::jsonb,
  error text
);

create table if not exists job_scrape.xing_search_definitions (
  id uuid primary key default gen_random_uuid(),
  name text not null unique,
  enabled boolean not null default true,
  keywords text not null,
  country_name text not null default '',
  location_text text not null default '',
  facets jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists job_scrape.xing_search_runs (
  id uuid primary key default gen_random_uuid(),
  crawl_run_id uuid not null references job_scrape.xing_crawl_runs(id),
  search_definition_id uuid not null references job_scrape.xing_search_definitions(id),
  status text not null default 'running',
  pages_fetched integer not null default 0,
  jobs_discovered integer not null default 0,
  blocked boolean not null default false,
  error text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  unique (crawl_run_id, search_definition_id)
);

create table if not exists job_scrape.xing_jobs (
  job_id text primary key,
  job_url text not null,
  is_external boolean not null default false,
  list_preview jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  is_active boolean not null default true,
  stale_since_at timestamptz,
  expired_at timestamptz,
  expire_reason text,
  last_seen_search_run_id uuid references job_scrape.xing_search_runs(id)
);

create table if not exists job_scrape.xing_job_search_hits (
  search_run_id uuid not null references job_scrape.xing_search_runs(id),
  job_id text not null references job_scrape.xing_jobs(job_id),
  rank integer not null,
  page_start integer not null,
  scraped_at timestamptz not null,
  primary key (search_run_id, job_id)
);

create table if not exists job_scrape.xing_job_details (
  job_id text primary key references job_scrape.xing_jobs(job_id),
  scraped_at timestamptz not null,
  posted_at_utc timestamptz,
  posted_time_ago text,
  job_title text,
  company_name text,
  job_location text,
  employment_type text,
  salary_range_text text,
  work_model text,
  job_description text,
  criteria jsonb not null default '{}'::jsonb,
  parse_ok boolean not null,
  last_error text,
  extracted_skills jsonb,
  extracted_skills_version integer,
  extracted_skills_extracted_at timestamptz
);

create index if not exists idx_xing_search_definitions_enabled on job_scrape.xing_search_definitions(enabled);
create index if not exists idx_xing_search_runs_crawl on job_scrape.xing_search_runs(crawl_run_id);
create index if not exists idx_xing_jobs_last_seen on job_scrape.xing_jobs(last_seen_at desc);
create index if not exists idx_xing_jobs_is_active_last_seen on job_scrape.xing_jobs(is_active, last_seen_at desc);
create index if not exists idx_xing_job_details_scraped_at on job_scrape.xing_job_details(scraped_at desc);

alter table if exists job_scrape.xing_jobs
  add column if not exists is_external boolean not null default false;
alter table if exists job_scrape.xing_jobs
  add column if not exists list_preview jsonb not null default '{}'::jsonb;
alter table if exists job_scrape.xing_jobs
  add column if not exists is_active boolean not null default true;
alter table if exists job_scrape.xing_jobs
  add column if not exists stale_since_at timestamptz;
alter table if exists job_scrape.xing_jobs
  add column if not exists expired_at timestamptz;
alter table if exists job_scrape.xing_jobs
  add column if not exists expire_reason text;
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        ensure_schema(conn)
        conn.commit()
    print("xing_tables_ready")


if __name__ == "__main__":
    main()
