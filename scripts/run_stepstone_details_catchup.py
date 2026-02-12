from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from scripts.db import connect


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_stepstone_details_catchup {ts}] {msg}", file=sys.stderr)


def _missing_details_count() -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                  from job_scrape.stepstone_jobs j
                  left join job_scrape.stepstone_job_details d
                    on d.job_id = j.job_id
                 where d.job_id is null
                """
            )
            (n,) = cur.fetchone()
    return int(n or 0)


def _cleanup_stale_running_runs(*, stale_minutes: int) -> list[str]:
    if stale_minutes <= 0:
        return []

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id
                  from job_scrape.stepstone_crawl_runs
                 where status = 'running'
                   and started_at < now() - (%s || ' minutes')::interval
                """,
                (str(stale_minutes),),
            )
            run_ids = [str(r[0]) for r in cur.fetchall()]

            for run_id in run_ids:
                cur.execute(
                    """
                    update job_scrape.stepstone_search_runs
                       set status = 'failed',
                           finished_at = now(),
                           error = coalesce(error, 'watchdog stale cleanup')
                     where crawl_run_id = %s
                       and status = 'running'
                    """,
                    (run_id,),
                )
                cur.execute(
                    """
                    update job_scrape.stepstone_crawl_runs
                       set status = 'failed',
                           finished_at = now(),
                           error = coalesce(error, 'watchdog stale cleanup')
                     where id = %s
                    """,
                    (run_id,),
                )
        conn.commit()

    return run_ids


def _run_single_batch(*, batch_size: int, timeout_seconds: int, trigger: str) -> dict:
    env = os.environ.copy()
    env["RUN_DISCOVERY"] = "0"
    env["RUN_DETAILS"] = "1"
    env["SYNC_SEARCH_DEFINITIONS_STEPSTONE"] = "0"
    # Catch-up loops run details only; table DDL checks can stall on locks and are unnecessary every batch.
    env.setdefault("ENSURE_STEPSTONE_TABLES", "0")
    env["MAX_JOB_DETAILS_PER_RUN"] = str(batch_size)
    env["CRAWL_TRIGGER"] = trigger
    # Keep details-script timeout aligned with outer batch timeout.
    env.setdefault("STEPSTONE_DETAILS_TIMEOUT_SECONDS", str(timeout_seconds))

    cmd = [sys.executable, "-m", "scripts.run_crawl_stepstone"]
    out = subprocess.check_output(
        cmd,
        env=env,
        text=True,
        stderr=sys.stderr,
        timeout=timeout_seconds,
    )
    line = ""
    for raw in reversed(out.splitlines()):
        if raw.strip():
            line = raw.strip()
            break
    if not line:
        raise RuntimeError("run_crawl_stepstone returned empty output")
    return json.loads(line)


def main() -> None:
    batch_size = int(os.getenv("STEPSTONE_CATCHUP_BATCH_SIZE", "300"))
    max_batches = int(os.getenv("STEPSTONE_CATCHUP_MAX_BATCHES", "100"))
    no_progress_limit = int(os.getenv("STEPSTONE_CATCHUP_NO_PROGRESS_LIMIT", "3"))
    stale_minutes = int(os.getenv("STEPSTONE_CATCHUP_STALE_MINUTES", "45"))
    batch_timeout_seconds = int(os.getenv("STEPSTONE_CATCHUP_BATCH_TIMEOUT_SECONDS", "10800"))
    sleep_seconds = int(os.getenv("STEPSTONE_CATCHUP_SLEEP_SECONDS", "5"))
    trigger = os.getenv("STEPSTONE_CATCHUP_TRIGGER", "manual_full_details_batch")

    missing_initial = _missing_details_count()
    _log(
        f"start missing={missing_initial} batch_size={batch_size} max_batches={max_batches} "
        f"no_progress_limit={no_progress_limit}"
    )

    no_progress_streak = 0
    batches_run = 0
    batches_failed = 0

    while batches_run < max_batches:
        stale_ids = _cleanup_stale_running_runs(stale_minutes=stale_minutes)
        if stale_ids:
            _log(f"watchdog cleaned stale running crawl_runs={stale_ids}")

        missing_before = _missing_details_count()
        if missing_before <= 0:
            break

        batches_run += 1
        _log(f"batch={batches_run} missing_before={missing_before}")

        try:
            result = _run_single_batch(
                batch_size=batch_size,
                timeout_seconds=batch_timeout_seconds,
                trigger=trigger,
            )
            status = str(result.get("status", "unknown"))
            _log(f"batch={batches_run} crawl_status={status}")
        except subprocess.TimeoutExpired:
            batches_failed += 1
            no_progress_streak += 1
            _log(f"batch={batches_run} timed out after {batch_timeout_seconds}s")
            if no_progress_streak >= no_progress_limit:
                break
            time.sleep(sleep_seconds)
            continue
        except Exception as e:
            batches_failed += 1
            no_progress_streak += 1
            _log(f"batch={batches_run} failed: {e}")
            if no_progress_streak >= no_progress_limit:
                break
            time.sleep(sleep_seconds)
            continue

        missing_after = _missing_details_count()
        delta = missing_before - missing_after
        _log(f"batch={batches_run} missing_after={missing_after} delta={delta}")

        if delta <= 0:
            no_progress_streak += 1
        else:
            no_progress_streak = 0

        if missing_after <= 0:
            break
        if no_progress_streak >= no_progress_limit:
            break
        time.sleep(sleep_seconds)

    missing_final = _missing_details_count()
    status = "success" if missing_final == 0 else "partial"
    out = {
        "status": status,
        "missing_initial": missing_initial,
        "missing_final": missing_final,
        "batches_run": batches_run,
        "batches_failed": batches_failed,
        "no_progress_streak": no_progress_streak,
    }
    print(json.dumps(out, ensure_ascii=False))

    if status != "success":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
