from __future__ import annotations

from scripts.db import connect


SQL = """
set statement_timeout = '30min';

-- Public API proxy views
--
-- Why: Supabase PostgREST exposes `public` by default. Exposing additional
-- schemas (like `job_scrape`) is optional and sometimes blocked by project
-- settings or user permissions.
--
-- These views keep the internal schema layout intact while making the same
-- read models available under `public.*` for the web app.

create or replace view public.jobs_dashboard_v as
select * from job_scrape.jobs_dashboard_v;

create or replace view public.target_jobs_v as
select * from job_scrape.target_jobs_m;

create or replace view public.working_student_jobs_v as
select * from job_scrape.working_student_jobs_v;

create or replace view public.jobs_dashboard_city_bubbles_v as
select * from job_scrape.jobs_dashboard_city_bubbles_m;

create or replace view public.working_student_city_bubbles_v as
select * from job_scrape.working_student_city_bubbles_m;

create or replace view public.jobs_dashboard_map_points_v as
select * from job_scrape.jobs_dashboard_map_points_m;

create or replace view public.working_student_map_points_v as
select * from job_scrape.working_student_map_points_m;

create or replace view public.jobs_dashboard_kpis_v as
select * from job_scrape.jobs_dashboard_kpis_v;

create or replace view public.working_student_kpis_v as
select * from job_scrape.working_student_kpis_v;

create or replace view public.jobs_dashboard_trend_v as
select * from job_scrape.jobs_dashboard_trend_m;

create or replace view public.working_student_trend_v as
select * from job_scrape.working_student_trend_m;

create or replace view public.jobs_dashboard_top_skills_v as
select * from job_scrape.jobs_dashboard_top_skills_m;

create or replace view public.working_student_top_skills_v as
select * from job_scrape.working_student_top_skills_m;

grant select on public.jobs_dashboard_v to anon, authenticated;
grant select on public.target_jobs_v to anon, authenticated;
grant select on public.working_student_jobs_v to anon, authenticated;
grant select on public.jobs_dashboard_city_bubbles_v to anon, authenticated;
grant select on public.working_student_city_bubbles_v to anon, authenticated;
grant select on public.jobs_dashboard_map_points_v to anon, authenticated;
grant select on public.working_student_map_points_v to anon, authenticated;
grant select on public.jobs_dashboard_kpis_v to anon, authenticated;
grant select on public.working_student_kpis_v to anon, authenticated;
grant select on public.jobs_dashboard_trend_v to anon, authenticated;
grant select on public.working_student_trend_v to anon, authenticated;
grant select on public.jobs_dashboard_top_skills_v to anon, authenticated;
grant select on public.working_student_top_skills_v to anon, authenticated;
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()


if __name__ == "__main__":
    main()
