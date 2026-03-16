from __future__ import annotations

from scripts.db import connect


SQL = """
create schema if not exists job_scrape;

set statement_timeout = '30min';

-- Lightweight analytics read models for the dashboard UI.

create or replace view job_scrape.jobs_dashboard_kpis_v as
with base as (
  select
    platform,
    job_id,
    company_name,
    first_seen_at,
    last_seen_at
  from job_scrape.jobs_dashboard_v
),
active_jobs as (
  -- "Active" is intentionally coarse; lifecycle maintenance can evolve later.
  select *
  from base
  where last_seen_at is not null
    and last_seen_at >= now() - interval '60 days'
),
remote_jobs as (
  select distinct c.platform, c.job_id
  from job_scrape.jobs_dashboard_location_candidates_v c
  join active_jobs a
    on a.platform = c.platform and a.job_id = c.job_id
  where c.token_kind = 'remote'
),
new_companies_7d as (
  select distinct btrim(company_name) as company_name
  from base
  where first_seen_at is not null
    and first_seen_at >= now() - interval '7 days'
    and company_name is not null
    and btrim(company_name) <> ''
)
select
  (select count(*)::bigint from active_jobs) as total_active_jobs,
  (select count(*)::bigint from remote_jobs) as remote_roles,
  (select count(*)::bigint from new_companies_7d) as new_companies_7d,
  now() as as_of
;

create or replace view job_scrape.jobs_dashboard_trend_v as
with base as (
  select
    date_trunc('month', coalesce(posted_at_utc, first_seen_at)) as bucket_start,
    platform,
    job_id
  from job_scrape.jobs_dashboard_v
  where coalesce(posted_at_utc, first_seen_at) is not null
),
agg as (
  select
    bucket_start,
    count(distinct (platform, job_id))::bigint as jobs_unique
  from base
  where bucket_start >= date_trunc('month', now() - interval '11 months')
  group by bucket_start
),
series as (
  select generate_series(
    date_trunc('month', now() - interval '11 months'),
    date_trunc('month', now()),
    interval '1 month'
  ) as bucket_start
)
select
  s.bucket_start,
  coalesce(a.jobs_unique, 0)::bigint as jobs_unique
from series s
left join agg a using (bucket_start)
order by s.bucket_start asc
;

-- Top skills for fixed windows (UI can pick 30d/90d without expensive client-side grouping).
create or replace view job_scrape.jobs_dashboard_top_skills_v as
with base as (
  select
    platform,
    job_id,
    coalesce(posted_at_utc, first_seen_at) as event_ts,
    extracted_skills
  from job_scrape.jobs_dashboard_v
  where coalesce(posted_at_utc, first_seen_at) is not null
),
skill_events as (
  select
    b.platform,
    b.job_id,
    b.event_ts,
    lower(btrim(sk.skill)) as skill
  from base b
  cross join lateral (
    select jsonb_array_elements_text(e.value) as skill
    from jsonb_each(coalesce(b.extracted_skills, '{}'::jsonb)) as e
    where jsonb_typeof(e.value) = 'array'
  ) sk
  where sk.skill is not null
    and btrim(sk.skill) <> ''
),
top_30 as (
  select
    30::int as days_window,
    se.skill,
    count(distinct (se.platform, se.job_id))::bigint as jobs_unique
  from skill_events se
  where se.event_ts >= now() - interval '30 days'
  group by se.skill
),
top_90 as (
  select
    90::int as days_window,
    se.skill,
    count(distinct (se.platform, se.job_id))::bigint as jobs_unique
  from skill_events se
  where se.event_ts >= now() - interval '90 days'
  group by se.skill
)
select * from top_30
union all
select * from top_90
;
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()


if __name__ == "__main__":
    main()
