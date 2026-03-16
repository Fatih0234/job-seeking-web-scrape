from __future__ import annotations

from scripts.db import connect


SQL_BLOCKS = [
    r"""
    create schema if not exists job_scrape;
    set statement_timeout = '30min';

    create materialized view if not exists job_scrape.jobs_dashboard_map_points_m as
    select *
    from job_scrape.jobs_dashboard_map_points_v
    with no data;

    refresh materialized view job_scrape.jobs_dashboard_map_points_m;

    create index if not exists idx_jdmp_kind_lat_lon
      on job_scrape.jobs_dashboard_map_points_m (token_kind, lat, lon);
    create index if not exists idx_jdmp_job
      on job_scrape.jobs_dashboard_map_points_m (platform, job_id);
    create index if not exists idx_jdmp_posted_at
      on job_scrape.jobs_dashboard_map_points_m (posted_at_utc);
    create index if not exists idx_jdmp_first_seen
      on job_scrape.jobs_dashboard_map_points_m (first_seen_at);
    """,
    r"""
    create schema if not exists job_scrape;
    set statement_timeout = '30min';

    create materialized view if not exists job_scrape.jobs_dashboard_city_bubbles_m as
    with pts as (
      select *
      from job_scrape.jobs_dashboard_map_points_m
      where token_kind = 'city'
        and lat is not null and lon is not null
    ),
    base as (
      select
        p.*,
        coalesce(p.posted_at_utc, p.first_seen_at) as event_ts,
        coalesce(nullif(p.geocode_city, ''), p.map_city_label) as bubble_label
      from pts p
    ),
    agg as (
      select
        'city'::text as token_kind,
        lat,
        lon,
        bubble_label as map_city_label,
        count(distinct (platform, job_id))::bigint as jobs_unique,
        sum(map_weight)::double precision as jobs_weighted,
        count(distinct (platform, job_id)) filter (where event_ts >= now() - interval '24 hours')::bigint as new_24h_unique,
        count(distinct (platform, job_id)) filter (where event_ts >= now() - interval '7 days')::bigint as new_7d_unique
      from base
      group by lat, lon, bubble_label
    )
    select
      a.*,
      tc.top_companies,
      ts.top_skills
    from agg a
    left join lateral (
      select array_agg(company_name order by cnt desc, company_name asc) as top_companies
      from (
        select
          b.company_name,
          count(distinct (b.platform, b.job_id))::bigint as cnt
        from base b
        where b.lat = a.lat and b.lon = a.lon and b.bubble_label = a.map_city_label
          and b.company_name is not null and btrim(b.company_name) <> ''
        group by b.company_name
        order by cnt desc, b.company_name asc
        limit 5
      ) s
    ) tc on true
    left join lateral (
      select array_agg(skill order by cnt desc, skill asc) as top_skills
      from (
        select
          skill,
          count(distinct (b.platform, b.job_id))::bigint as cnt
        from base b
        cross join lateral (
          select jsonb_array_elements_text(e.value) as skill
          from jsonb_each(coalesce(b.extracted_skills, '{}'::jsonb)) as e
          where jsonb_typeof(e.value) = 'array'
        ) sk
        where b.lat = a.lat and b.lon = a.lon and b.bubble_label = a.map_city_label
          and skill is not null and btrim(skill) <> ''
        group by skill
        order by cnt desc, skill asc
        limit 10
      ) s
    ) ts on true
    with no data;

    refresh materialized view job_scrape.jobs_dashboard_city_bubbles_m;

    create index if not exists idx_jdcb_lat_lon
      on job_scrape.jobs_dashboard_city_bubbles_m (lat, lon);
    create index if not exists idx_jdcb_new7d
      on job_scrape.jobs_dashboard_city_bubbles_m (new_7d_unique desc);
    create index if not exists idx_jdcb_new24h
      on job_scrape.jobs_dashboard_city_bubbles_m (new_24h_unique desc);
    """,
    r"""
    create schema if not exists job_scrape;
    set statement_timeout = '30min';

    create materialized view if not exists job_scrape.jobs_dashboard_trend_m as
    select *
    from job_scrape.jobs_dashboard_trend_v
    with no data;
    refresh materialized view job_scrape.jobs_dashboard_trend_m;

    create materialized view if not exists job_scrape.jobs_dashboard_top_skills_m as
    select *
    from job_scrape.jobs_dashboard_top_skills_v
    with no data;
    refresh materialized view job_scrape.jobs_dashboard_top_skills_m;

    create index if not exists idx_jdts_days_jobs
      on job_scrape.jobs_dashboard_top_skills_m (days_window, jobs_unique desc);
    """,
]


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            for sql in SQL_BLOCKS:
                cur.execute(sql)
                conn.commit()


if __name__ == "__main__":
    main()
