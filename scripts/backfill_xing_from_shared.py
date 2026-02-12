from __future__ import annotations

import json

from scripts.create_xing_tables import SQL as XING_TABLES_SQL
from scripts.db import connect


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(XING_TABLES_SQL)

            cur.execute(
                """
                insert into job_scrape.xing_search_definitions
                  (id, name, enabled, keywords, country_name, location_text, facets, created_at, updated_at)
                select
                  id,
                  name,
                  enabled,
                  keywords,
                  coalesce(country_name, ''),
                  coalesce(location_text, ''),
                  coalesce(facets, '{}'::jsonb),
                  created_at,
                  updated_at
                from job_scrape.search_definitions
                where source = 'xing'
                on conflict (id) do update set
                  name = excluded.name,
                  enabled = excluded.enabled,
                  keywords = excluded.keywords,
                  country_name = excluded.country_name,
                  location_text = excluded.location_text,
                  facets = excluded.facets,
                  updated_at = excluded.updated_at
                """
            )

            cur.execute(
                """
                insert into job_scrape.xing_crawl_runs
                  (id, started_at, finished_at, trigger, status, stats, error)
                select distinct
                  cr.id,
                  cr.started_at,
                  cr.finished_at,
                  cr.trigger,
                  cr.status,
                  coalesce(cr.stats, '{}'::jsonb),
                  cr.error
                from job_scrape.crawl_runs cr
                join job_scrape.search_runs sr on sr.crawl_run_id = cr.id
                join job_scrape.search_definitions sd on sd.id = sr.search_definition_id
                where sd.source = 'xing'
                on conflict (id) do update set
                  started_at = excluded.started_at,
                  finished_at = excluded.finished_at,
                  trigger = excluded.trigger,
                  status = excluded.status,
                  stats = excluded.stats,
                  error = excluded.error
                """
            )

            cur.execute(
                """
                insert into job_scrape.xing_search_runs
                  (id, crawl_run_id, search_definition_id, status, pages_fetched, jobs_discovered, blocked, error, started_at, finished_at)
                select
                  sr.id,
                  sr.crawl_run_id,
                  sr.search_definition_id,
                  sr.status,
                  sr.pages_fetched,
                  sr.jobs_discovered,
                  sr.blocked,
                  sr.error,
                  sr.started_at,
                  sr.finished_at
                from job_scrape.search_runs sr
                join job_scrape.search_definitions sd on sd.id = sr.search_definition_id
                where sd.source = 'xing'
                on conflict (id) do update set
                  crawl_run_id = excluded.crawl_run_id,
                  search_definition_id = excluded.search_definition_id,
                  status = excluded.status,
                  pages_fetched = excluded.pages_fetched,
                  jobs_discovered = excluded.jobs_discovered,
                  blocked = excluded.blocked,
                  error = excluded.error,
                  started_at = excluded.started_at,
                  finished_at = excluded.finished_at
                """
            )

            cur.execute(
                """
                insert into job_scrape.xing_jobs
                  (job_id, job_url, is_external, list_preview, first_seen_at, last_seen_at, last_seen_search_run_id)
                select
                  j.job_id,
                  j.job_url,
                  (j.job_id like 'ext_%') as is_external,
                  '{}'::jsonb,
                  j.first_seen_at,
                  j.last_seen_at,
                  j.last_seen_search_run_id
                from job_scrape.jobs j
                where j.source = 'xing'
                on conflict (job_id) do update set
                  job_url = excluded.job_url,
                  is_external = excluded.is_external,
                  first_seen_at = least(job_scrape.xing_jobs.first_seen_at, excluded.first_seen_at),
                  last_seen_at = greatest(job_scrape.xing_jobs.last_seen_at, excluded.last_seen_at),
                  last_seen_search_run_id = excluded.last_seen_search_run_id
                """
            )

            cur.execute(
                """
                insert into job_scrape.xing_job_search_hits
                  (search_run_id, job_id, rank, page_start, scraped_at)
                select
                  h.search_run_id,
                  h.job_id,
                  h.rank,
                  h.page_start,
                  h.scraped_at
                from job_scrape.job_search_hits h
                where h.source = 'xing'
                  and exists (select 1 from job_scrape.xing_search_runs xsr where xsr.id = h.search_run_id)
                  and exists (select 1 from job_scrape.xing_jobs xj where xj.job_id = h.job_id)
                on conflict do nothing
                """
            )

            cur.execute(
                """
                insert into job_scrape.xing_job_details
                  (job_id, scraped_at, posted_at_utc, posted_time_ago,
                   job_title, company_name, job_location,
                   employment_type, salary_range_text, work_model,
                   job_description, criteria, parse_ok, last_error,
                   extracted_skills, extracted_skills_version, extracted_skills_extracted_at)
                select
                  d.job_id,
                  d.scraped_at,
                  null::timestamptz as posted_at_utc,
                  d.posted_time_ago,
                  d.job_title,
                  d.company_name,
                  d.job_location,
                  coalesce(d.criteria->>'employment_type', d.criteria->>'employmentType') as employment_type,
                  coalesce(d.criteria->>'salary_range_text', d.criteria->>'salary') as salary_range_text,
                  coalesce(d.criteria->>'work_model', d.criteria->>'workType') as work_model,
                  d.job_description,
                  coalesce(d.criteria, '{}'::jsonb),
                  d.parse_ok,
                  d.last_error,
                  d.extracted_skills,
                  d.extracted_skills_version,
                  d.extracted_skills_extracted_at
                from job_scrape.job_details d
                where d.source = 'xing'
                  and exists (select 1 from job_scrape.xing_jobs xj where xj.job_id = d.job_id)
                on conflict (job_id) do update set
                  scraped_at = excluded.scraped_at,
                  posted_time_ago = excluded.posted_time_ago,
                  job_title = excluded.job_title,
                  company_name = excluded.company_name,
                  job_location = excluded.job_location,
                  employment_type = excluded.employment_type,
                  salary_range_text = excluded.salary_range_text,
                  work_model = excluded.work_model,
                  job_description = excluded.job_description,
                  criteria = excluded.criteria,
                  parse_ok = excluded.parse_ok,
                  last_error = excluded.last_error,
                  extracted_skills = excluded.extracted_skills,
                  extracted_skills_version = excluded.extracted_skills_version,
                  extracted_skills_extracted_at = excluded.extracted_skills_extracted_at
                """
            )

            cur.execute("select count(*) from job_scrape.xing_search_definitions")
            (search_defs_total,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.xing_search_runs")
            (search_runs_total,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.xing_jobs")
            (jobs_total,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.xing_job_search_hits")
            (hits_total,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.xing_job_details")
            (details_total,) = cur.fetchone()

        conn.commit()

    print(
        json.dumps(
            {
                "status": "success",
                "counts": {
                    "search_definitions": int(search_defs_total or 0),
                    "search_runs": int(search_runs_total or 0),
                    "jobs": int(jobs_total or 0),
                    "job_search_hits": int(hits_total or 0),
                    "job_details": int(details_total or 0),
                },
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
