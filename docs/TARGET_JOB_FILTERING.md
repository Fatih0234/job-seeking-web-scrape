# Target Job Filtering

This repo now includes a dedicated target-job classification layer for the
Germany-focused application workflow.

## Objects

- Audit/debug view: `job_scrape.jobs_target_classification_v`
  - One row per job across LinkedIn, Stepstone, and XING
  - Includes canonical dashboard fields plus:
    - `is_active`
    - `employment_type`
    - `contract_type`
    - `work_type`
    - `part_time_flag`
    - `is_working_student`
    - `is_internship`
    - `is_part_time`
    - `is_target_role`
    - `match_sources`
    - `match_reasons`
    - `target_role_confidence`
    - `target_role_confidence_rank`
- Filtered live view: `job_scrape.target_jobs_v`
  - `select * from job_scrape.jobs_target_classification_v where is_target_role`
- Filtered materialized view: `job_scrape.target_jobs_m`
  - Fast read model for repeated queries
- Public proxy: `public.target_jobs_v`
  - Selects from `job_scrape.target_jobs_m`
- Conservative app base view: `job_scrape.working_student_jobs_v`
  - Selects only active working-student rows with `target_role_confidence in ('high', 'medium')`
- Conservative app public proxy: `public.working_student_jobs_v`
  - Used by the current web app APIs for candidate-focused browsing

## Rule Design

- Structured fields are evaluated first.
- Title matching is evaluated second.
- Description matching is evaluated third.
- Supported languages: German + English.

The current rule catalog targets:

- Working student:
  - `werkstudent`, `werkstudent:in`, `studentische hilfskraft`,
    `studentische aushilfe`, `studentische mitarbeiter:in`,
    `studentenjob`, `working student`, `student assistant`,
    `student employee`, `student worker`
- Internship:
  - `praktikum`, `praktikant:in`, `pflichtpraktikum`,
    `freiwilliges praktikum`, `internship`, `intern`, `praxissemester`
- Part-time:
  - `teilzeit`, `teilzeitstelle`, `teilzeitjob`, `part-time`, `part time`

Platform-specific structured logic:

- LinkedIn:
  - `criteria.employment_type='Internship'` => internship
  - `criteria.employment_type in ('Part-time', 'Part time')` => part-time
- Stepstone:
  - `contract_type`, `work_type`, and `part_time` are all used
- XING:
  - `employment_type in ('Student', 'For students')` => working student
  - `employment_type in ('Part-time', 'Part time')` => part-time

## Confidence

- `high`
  - Any structured match
  - Any title match
- `medium`
  - Description-only match with no contradictory structured signal
- `low`
  - Description-only working-student or internship match with contradictory
    structured non-target signals such as `Full-time`, `Vollzeit`,
    `Permanent contract`, or `Feste Anstellung`

`is_target_role` includes `low` rows so nothing is hidden, but the recommended
default workflow is to query only `high` and `medium`.

## Create Or Refresh

```bash
source .venv/bin/activate
python -m scripts.create_target_job_views
```

Or refresh the full read-model stack:

```bash
source .venv/bin/activate
python -m scripts.refresh_dashboard_read_models
```

## Example Queries

Only active working-student roles:

```sql
select *
from public.target_jobs_v
where is_working_student
  and is_active
  and target_role_confidence in ('high', 'medium')
order by coalesce(posted_at_utc, first_seen_at) desc nulls last;
```

Only active internships:

```sql
select *
from public.target_jobs_v
where is_internship
  and is_active
  and target_role_confidence in ('high', 'medium')
order by coalesce(posted_at_utc, first_seen_at) desc nulls last;
```

Only active part-time jobs:

```sql
select *
from public.target_jobs_v
where is_part_time
  and is_active
  and target_role_confidence in ('high', 'medium')
order by coalesce(posted_at_utc, first_seen_at) desc nulls last;
```

All target roles ordered by freshness:

```sql
select *
from public.target_jobs_v
where is_target_role
order by coalesce(posted_at_utc, first_seen_at) desc nulls last;
```

Low-confidence review queue:

```sql
select platform, job_id, job_title, company_name, match_sources, match_reasons
from job_scrape.jobs_target_classification_v
where is_target_role
  and target_role_confidence = 'low'
order by coalesce(posted_at_utc, first_seen_at) desc nulls last;
```
