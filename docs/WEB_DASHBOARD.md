# Web Dashboard (GeoWorks)

The Next.js app lives at:

- `/Volumes/T7/job-seeking-web-scrape/web`

It reads from Supabase **views** (no writes/auth in MVP).

## Required Supabase Views

- Canonical jobs: `job_scrape.jobs_dashboard_v`
- Target jobs:
  - `job_scrape.target_jobs_v`
  - `job_scrape.target_jobs_m`
- Conservative working-student app views:
  - `job_scrape.working_student_jobs_v`
  - `job_scrape.working_student_map_points_v`
  - `job_scrape.working_student_city_bubbles_v`
  - `job_scrape.working_student_kpis_v`
  - `job_scrape.working_student_trend_v`
  - `job_scrape.working_student_top_skills_v`
- Map:
  - `job_scrape.jobs_dashboard_city_bubbles_v`
  - `job_scrape.jobs_dashboard_map_points_v`
- Analytics:
  - `job_scrape.jobs_dashboard_kpis_v`
  - `job_scrape.jobs_dashboard_trend_v`
  - `job_scrape.jobs_dashboard_top_skills_v`

To create/update the base live views in DB:

```bash
source .venv/bin/activate
python -m scripts.create_dashboard_view
python -m scripts.create_target_job_views
python -m scripts.create_working_student_app_views
python -m scripts.create_dashboard_map_view
python -m scripts.create_dashboard_analytics_views
```

To refresh the web-facing read models that the app queries in production:

```bash
source .venv/bin/activate
python -m scripts.refresh_dashboard_read_models
```

This refreshes the base dashboard views, materialized dashboard views, and the
`public.*` proxy views in dependency order. It also refreshes the target-job
materialized view used for focused application workflows. Run it after workflows or manual
operations that change jobs, timestamps, extracted skills, or geocodes.

## Supabase API Exposure + SELECT Grants

You have two options:

### Option A (recommended): Keep PostgREST exposed schemas minimal (use public proxy views)

This repo includes `/Volumes/T7/job-seeking-web-scrape/scripts/create_public_api_views.py`, which creates
`public.*` proxy views that select from the internal `job_scrape.*` views.

Why: `public` is exposed by default on Supabase (no PostgREST schema config needed).

Run:

```bash
source .venv/bin/activate
python -m scripts.refresh_dashboard_read_models
```

This refresh also re-creates the `public.*` proxies and grants `SELECT` on them
to `anon, authenticated`.

### Option B: Expose `job_scrape` directly (requires Supabase API config)

1. Supabase Dashboard:
   - API -> Exposed schemas: add `job_scrape`

2. Run once in the Supabase SQL editor (adjust roles if you don't use `anon/authenticated`):

```sql
grant usage on schema job_scrape to anon, authenticated;

grant select on job_scrape.jobs_dashboard_v to anon, authenticated;
grant select on job_scrape.target_jobs_v to anon, authenticated;
grant select on job_scrape.working_student_jobs_v to anon, authenticated;
grant select on job_scrape.jobs_dashboard_city_bubbles_v to anon, authenticated;
grant select on job_scrape.working_student_city_bubbles_v to anon, authenticated;
grant select on job_scrape.jobs_dashboard_map_points_v to anon, authenticated;
grant select on job_scrape.working_student_map_points_v to anon, authenticated;

grant select on job_scrape.jobs_dashboard_kpis_v to anon, authenticated;
grant select on job_scrape.working_student_kpis_v to anon, authenticated;
grant select on job_scrape.jobs_dashboard_trend_v to anon, authenticated;
grant select on job_scrape.working_student_trend_v to anon, authenticated;
grant select on job_scrape.jobs_dashboard_top_skills_v to anon, authenticated;
grant select on job_scrape.working_student_top_skills_v to anon, authenticated;
```

## Local Dev

Environment variables (see `/Volumes/T7/job-seeking-web-scrape/.env.example`):

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`

Run:

```bash
cd web
npm install
npm run dev
```
