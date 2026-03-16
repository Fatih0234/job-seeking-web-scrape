# XING Cron Reconciliation Runbook

## Purpose
Operational playbook for investigating and fixing XING workflow vs Supabase inconsistencies.

## Default Guardrails
- Strict verification runs in both workflows via `scripts.verify_xing_workflow_run`.
- Stale-running auto-heal is enabled (`--repair-stale 1`).
- Stale threshold is `180` minutes.

## Quick Health Check
Run a 14-day reconciliation:

```bash
python -m scripts.xing_cron_diagnostics --days 14 --strict 1
```

Key signals:
- `db.stale_running_crawl_runs`
- `db.inconsistent_running_search_runs`
- `mismatches[]`

## Failure Classes
1. `gh_success_without_db_row`
- Meaning: GitHub run finished `success` but no matching `job_scrape.xing_crawl_runs` row exists.
- Likely causes: run never reached crawl phase, or wrong trigger mapping.

2. `gh_non_success_with_lingering_running_db_row`
- Meaning: GitHub run was `failure/cancelled/timed_out`, but matching DB run is still `running`.
- Likely causes: timeout/cancel before finalization, signal path missed, DB write failure in finalization.

3. `db_schedule_without_gh_run`
- Meaning: scheduled DB crawl run has no matching scheduled GitHub run.
- Likely causes: manual/scripted DB invocation, timestamp skew beyond match window, GH retention/reporting gaps.

4. `stale_rows_remain` (verifier output)
- Meaning: stale `running` rows still exist even after auto-heal attempt.
- Likely causes: DB permissions/query failure, race with in-flight runs, invalid stale threshold.

5. `lingering_running_after_non_success` (verifier output)
- Meaning: non-success workflow outcome left one or more `running` rows in the workflow window.
- Likely causes: crash/cancel path did not finalize, cleanup not invoked.

## Manual Repair (If Needed)
Use only when strict verifier cannot auto-heal:

```sql
with stale as (
  select id
  from job_scrape.xing_crawl_runs
  where status = 'running'
    and started_at < now() - interval '180 minutes'
)
update job_scrape.xing_search_runs sr
set status = 'failed',
    finished_at = now(),
    error = coalesce(sr.error, 'manual_repair_stale_running')
where sr.status = 'running'
  and sr.crawl_run_id in (select id from stale);
```

```sql
with stale as (
  select id
  from job_scrape.xing_crawl_runs
  where status = 'running'
    and started_at < now() - interval '180 minutes'
)
update job_scrape.xing_crawl_runs cr
set status = 'failed',
    finished_at = now(),
    error = coalesce(cr.error, 'manual_repair_stale_running')
where cr.id in (select id from stale);
```

## Post-Repair Acceptance
- `stale_running_crawl_runs = 0`
- `inconsistent_running_search_runs = 0`
- Next scheduled runs produce matching GitHub+DB rows with terminal DB status (`success|blocked|failed`).
