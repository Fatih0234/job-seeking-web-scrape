from __future__ import annotations

from scripts.db import connect


SQL = """
create schema if not exists job_scrape;

create table if not exists job_scrape.location_geocode_cache (
  id uuid primary key default gen_random_uuid(),
  provider text not null default 'geoapify',
  location_text_raw text not null,
  location_text_norm text not null,
  country_scope text not null,
  status text not null default 'pending',
  attempt_count integer not null default 0,
  last_attempted_at timestamptz,
  next_retry_at timestamptz not null default now(),
  resolved_at timestamptz,
  lat double precision,
  lon double precision,
  formatted text,
  result_type text,
  rank_confidence double precision,
  rank_importance double precision,
  city text,
  state text,
  country text,
  country_code text,
  timezone_name text,
  query_text_returned text,
  raw_response jsonb,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (provider, location_text_norm, country_scope),
  check (status in ('pending', 'resolved', 'no_match', 'error', 'failed_permanent'))
);

create index if not exists idx_location_geocode_cache_status_retry
  on job_scrape.location_geocode_cache(status, next_retry_at);

create index if not exists idx_location_geocode_cache_country_scope
  on job_scrape.location_geocode_cache(country_scope);

create index if not exists idx_location_geocode_cache_lat_lon
  on job_scrape.location_geocode_cache(lat, lon)
  where lat is not null and lon is not null;
"""


def ensure_schema() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()


def main() -> None:
    ensure_schema()
    print("geocode_schema_ready")


if __name__ == "__main__":
    main()
