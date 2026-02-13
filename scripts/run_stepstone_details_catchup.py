from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.db import connect
from scripts.stepstone_crawl_common import fail_running_search_runs, finish_crawl_run


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_stepstone_details_catchup {ts}] {msg}", file=sys.stderr)


def _detail_last_seen_window_days() -> int:
    return int(os.getenv("DETAIL_LAST_SEEN_WINDOW_DAYS") or os.getenv("STEPSTONE_CATCHUP_DETAIL_LAST_SEEN_WINDOW_DAYS") or "60")


def _detail_staleness_days() -> int:
    # Catch-up is primarily to fill missing details; avoid constant refresh churn by default.
    return int(os.getenv("DETAIL_STALENESS_DAYS") or os.getenv("STEPSTONE_CATCHUP_DETAIL_STALENESS_DAYS") or "365")


def _detail_blocked_retry_hours() -> int:
    return int(os.getenv("DETAIL_BLOCKED_RETRY_HOURS") or os.getenv("STEPSTONE_CATCHUP_DETAIL_BLOCKED_RETRY_HOURS") or "24")


def _missing_details_count() -> int:
    """
    Count remaining detail work using the same selection constraints as run_details_stepstone.
    """
    last_seen_window_days = _detail_last_seen_window_days()
    staleness_days = _detail_staleness_days()
    blocked_retry_hours = _detail_blocked_retry_hours()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                  from job_scrape.stepstone_jobs j
                  left join job_scrape.stepstone_job_details d
                    on d.job_id = j.job_id
                 where j.last_seen_at > now() - (%s || ' days')::interval
                   and (
                        d.job_id is null
                        or d.scraped_at < now() - (%s || ' days')::interval
                        or (
                            d.last_error = 'blocked'
                            and d.scraped_at < now() - (%s || ' hours')::interval
                        )
                   )
                """
                ,
                (str(last_seen_window_days), str(staleness_days), str(blocked_retry_hours)),
            )
            (n,) = cur.fetchone()
    return int(n or 0)


def _finalize_stale_run_from_details_jsonl(*, run_id: str, details_jsonl: Path) -> None:
    """
    Import a completed details JSONL and finalize a stale crawl_run.

    Note: On GitHub Actions, stale runs typically won't have their JSONL present on the next runner.
    This recovery path mainly helps long-running local/VM executions where output/ persists.
    """
    timeout_seconds = int(os.getenv("DETAIL_IMPORT_TIMEOUT_SECONDS", "1800"))
    cmd = [sys.executable, "-m", "scripts.import_details_stepstone", str(details_jsonl)]
    out = subprocess.check_output(cmd, text=True, timeout=timeout_seconds)
    imp = json.loads(out.strip())

    details_status = str(imp.get("status") or "unknown")
    crawl_status = "success"
    crawl_error = None
    if details_status == "blocked":
        crawl_status = "blocked"
    if details_status == "failed":
        crawl_status = "failed"
        crawl_error = str(imp.get("error") or "details import reported failed during stale recovery")

    stats = {
        "discovery": {"status": "skipped"},
        "details": {k: v for k, v in imp.items() if k != "crawl_run_id"},
    }

    try:
        fail_running_search_runs(run_id, error="stale recovery finalize")
    except Exception:
        pass

    finish_crawl_run(run_id, status=crawl_status, stats=stats, error=crawl_error)


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
        conn.commit()

    cleaned: list[str] = []
    for run_id in run_ids:
        details_jsonl = Path("output") / f"stepstone_details_{run_id}.jsonl"
        if details_jsonl.exists() and details_jsonl.stat().st_size > 0:
            try:
                _log(f"stale recovery: importing {details_jsonl}")
                _finalize_stale_run_from_details_jsonl(run_id=run_id, details_jsonl=details_jsonl)
                cleaned.append(run_id)
                continue
            except Exception as e:
                reason = f"watchdog stale recovery import failed: {e}"
                try:
                    fail_running_search_runs(run_id, error=reason)
                except Exception:
                    pass
                finish_crawl_run(run_id, status="failed", stats={}, error=reason)
                cleaned.append(run_id)
                continue

        reason = "watchdog stale cleanup (no completed details JSONL available)"
        try:
            fail_running_search_runs(run_id, error=reason)
        except Exception:
            pass
        finish_crawl_run(run_id, status="failed", stats={}, error=reason)
        cleaned.append(run_id)

    return cleaned


def _run_single_batch(*, batch_size: int, timeout_seconds: int, trigger: str) -> dict:
    env = os.environ.copy()
    env["RUN_DISCOVERY"] = "0"
    env["RUN_DETAILS"] = "1"
    env["SYNC_SEARCH_DEFINITIONS_STEPSTONE"] = "0"
    # Catch-up loops run details only; table DDL checks can stall on locks and are unnecessary every batch.
    env.setdefault("ENSURE_STEPSTONE_TABLES", "0")
    env["MAX_JOB_DETAILS_PER_RUN"] = str(batch_size)
    env["CRAWL_TRIGGER"] = trigger
    env.setdefault("DETAIL_LAST_SEEN_WINDOW_DAYS", str(_detail_last_seen_window_days()))
    env.setdefault("DETAIL_STALENESS_DAYS", str(_detail_staleness_days()))
    env.setdefault("DETAIL_BLOCKED_RETRY_HOURS", str(_detail_blocked_retry_hours()))
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
    max_total_seconds = int(os.getenv("STEPSTONE_CATCHUP_MAX_TOTAL_SECONDS", "0"))
    sleep_seconds = int(os.getenv("STEPSTONE_CATCHUP_SLEEP_SECONDS", "5"))
    trigger = os.getenv("STEPSTONE_CATCHUP_TRIGGER", "manual_full_details_batch")
    strict = (os.getenv("STEPSTONE_CATCHUP_STRICT", "0") or "0").strip().lower() in {"1", "true", "yes"}

    started = time.monotonic()
    missing_initial = _missing_details_count()
    _log(
        "selection constraints "
        f"DETAIL_LAST_SEEN_WINDOW_DAYS={_detail_last_seen_window_days()} "
        f"DETAIL_STALENESS_DAYS={_detail_staleness_days()} "
        f"DETAIL_BLOCKED_RETRY_HOURS={_detail_blocked_retry_hours()}"
    )
    _log(
        f"start missing={missing_initial} batch_size={batch_size} max_batches={max_batches} "
        f"no_progress_limit={no_progress_limit}"
    )
    if max_total_seconds > 0:
        _log(f"max_total_seconds={max_total_seconds}")

    no_progress_streak = 0
    batches_run = 0
    batches_failed = 0
    blocked_encountered = False

    while batches_run < max_batches:
        if max_total_seconds > 0 and (time.monotonic() - started) > max_total_seconds:
            _log("max_total_seconds reached; stopping catch-up early to keep cron bounded")
            break

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
            if status == "blocked":
                blocked_encountered = True
            if status == "failed":
                batches_failed += 1
                no_progress_streak += 1
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

        if blocked_encountered:
            _log("blocked detected; stopping catch-up early to retry later")
            break

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
    status = "success" if missing_final == 0 else ("blocked" if blocked_encountered else "partial")
    out = {
        "status": status,
        "missing_initial": missing_initial,
        "missing_final": missing_final,
        "selection_constraints": {
            "detail_last_seen_window_days": _detail_last_seen_window_days(),
            "detail_staleness_days": _detail_staleness_days(),
            "detail_blocked_retry_hours": _detail_blocked_retry_hours(),
        },
        "batches_run": batches_run,
        "batches_failed": batches_failed,
        "no_progress_streak": no_progress_streak,
    }
    print(json.dumps(out, ensure_ascii=False))

    if strict and status != "success":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
