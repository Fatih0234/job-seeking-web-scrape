from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.db import connect, now_utc_iso


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_details_stepstone {ts}] {msg}", file=sys.stderr)


def _stop_process_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        return
    try:
        proc.wait(timeout=10)
        return
    except Exception:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        pass


def _apply_scrapy_speed_overrides(cmd: list[str], env: dict[str, str]) -> list[str]:
    """
    Allow runtime speed tuning without changing safe defaults.
    """
    mapping = (
        ("STEPSTONE_DETAIL_CONCURRENCY", "CONCURRENT_REQUESTS"),
        ("STEPSTONE_DETAIL_CONCURRENT_PER_DOMAIN", "CONCURRENT_REQUESTS_PER_DOMAIN"),
        ("STEPSTONE_DETAIL_DOWNLOAD_DELAY_SECONDS", "DOWNLOAD_DELAY"),
        ("STEPSTONE_DETAIL_DOWNLOAD_TIMEOUT_SECONDS", "DOWNLOAD_TIMEOUT"),
        ("STEPSTONE_DETAIL_RANDOMIZE_DOWNLOAD_DELAY", "RANDOMIZE_DOWNLOAD_DELAY"),
    )

    applied: list[str] = []
    for env_key, scrapy_key in mapping:
        raw = (env.get(env_key) or "").strip()
        if not raw:
            continue
        cmd.extend(["-s", f"{scrapy_key}={raw}"])
        applied.append(f"{scrapy_key}={raw}")

    if applied:
        _log("applying speed overrides: " + ", ".join(applied))
    return cmd


def select_jobs_for_details(
    *, limit: int, staleness_days: int, blocked_retry_hours: int, last_seen_window_days: int
) -> list[dict]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.job_id, j.job_url
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
                 order by (d.job_id is null) desc, d.scraped_at asc nulls first, j.last_seen_at desc
                 limit %s
                """,
                (str(last_seen_window_days), str(staleness_days), str(blocked_retry_hours), limit),
            )
            rows = cur.fetchall()

    return [{"source": "stepstone", "job_id": r[0], "job_url": r[1]} for r in rows]


def run_spider(*, crawl_run_id: str, jobs: list[dict], out_jsonl: Path) -> Path:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    inputs = {"crawl_run_id": crawl_run_id, "generated_at": now_utc_iso(), "jobs": jobs}
    inputs_path = out_jsonl.with_suffix(".inputs.json")
    inputs_path.write_text(json.dumps(inputs, ensure_ascii=False, indent=2), encoding="utf-8")

    env = os.environ.copy()
    env.setdefault("DETAIL_DEBUG_FAILURE_LIMIT", "5")
    env.setdefault("CIRCUIT_BREAKER_BLOCKS", "3")
    spider_timeout_seconds = int(env.get("DETAIL_SPIDER_TIMEOUT_SECONDS", "7200"))
    progress_timeout_seconds = int(env.get("DETAIL_PROGRESS_TIMEOUT_SECONDS", "240"))

    cmd = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "stepstone_job_detail_batch",
        "-a",
        f"inputs={inputs_path}",
        "-a",
        f"crawl_run_id={crawl_run_id}",
        "-O",
        str(out_jsonl),
        "-s",
        "LOG_LEVEL=INFO",
    ]
    cmd = _apply_scrapy_speed_overrides(cmd, env)
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=sys.stderr,
        stderr=sys.stderr,
        start_new_session=True,
    )

    started = time.monotonic()
    last_progress = started
    last_size = -1
    while True:
        now = time.monotonic()
        size = out_jsonl.stat().st_size if out_jsonl.exists() else 0
        if size > last_size:
            last_size = size
            last_progress = now

        rc = proc.poll()
        if rc is not None:
            if rc != 0:
                raise RuntimeError(f"Stepstone detail spider failed (exit={rc}). See Scrapy logs above.")
            break

        if now - started > spider_timeout_seconds:
            _stop_process_group(proc)
            raise RuntimeError(
                f"Stepstone detail spider timed out after {spider_timeout_seconds}s; aborting this batch safely."
            )

        if now - last_progress > progress_timeout_seconds:
            _stop_process_group(proc)
            raise RuntimeError(
                f"Stepstone detail spider made no output progress for {progress_timeout_seconds}s; "
                "aborting this batch safely."
            )

        time.sleep(5)

    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_details_stepstone", str(jsonl_path)]
    timeout_seconds = int(os.getenv("DETAIL_IMPORT_TIMEOUT_SECONDS", "1800"))
    try:
        out = subprocess.check_output(cmd, text=True, timeout=timeout_seconds)
        return json.loads(out.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"import_details_stepstone failed (exit={e.returncode}).") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"import_details_stepstone timed out after {timeout_seconds}s.") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("import_details_stepstone did not return valid JSON") from e


def main() -> None:
    crawl_run_id = os.getenv("CRAWL_RUN_ID")
    if not crawl_run_id:
        raise SystemExit("CRAWL_RUN_ID env var is required (use scripts/run_crawl_stepstone.py to orchestrate)")

    limit = int(os.getenv("MAX_JOB_DETAILS_PER_RUN", "200"))
    staleness_days = int(os.getenv("DETAIL_STALENESS_DAYS", "7"))
    blocked_retry_hours = int(os.getenv("DETAIL_BLOCKED_RETRY_HOURS", "24"))
    last_seen_window_days = int(os.getenv("DETAIL_LAST_SEEN_WINDOW_DAYS", "60"))
    _log(
        "selecting jobs "
        f"limit={limit} staleness_days={staleness_days} blocked_retry_hours={blocked_retry_hours} "
        f"last_seen_window_days={last_seen_window_days}"
    )

    jobs = select_jobs_for_details(
        limit=limit,
        staleness_days=staleness_days,
        blocked_retry_hours=blocked_retry_hours,
        last_seen_window_days=last_seen_window_days,
    )
    if not jobs:
        _log("selected 0 jobs (nothing to do)")
        print(json.dumps({"status": "success", "crawl_run_id": crawl_run_id, "counts": {"detail_jobs_selected": 0}}))
        return

    _log(f"selected {len(jobs)} jobs for details")
    out_jsonl = Path("output") / f"stepstone_details_{crawl_run_id}.jsonl"
    spider_error: str | None = None
    try:
        run_spider(crawl_run_id=crawl_run_id, jobs=jobs, out_jsonl=out_jsonl)
    except Exception as e:
        spider_error = str(e)
        _log(f"detail spider ended with error; attempting partial import if available: {spider_error}")

    if not out_jsonl.exists() or out_jsonl.stat().st_size <= 0:
        if spider_error:
            raise RuntimeError(spider_error)
        raise RuntimeError("detail spider produced no output JSONL")

    stats = import_results(out_jsonl)
    stats.setdefault("counts", {})
    stats["counts"]["detail_jobs_selected"] = len(jobs)
    if spider_error:
        stats["status"] = "failed"
        stats["error"] = spider_error
    _log(
        "imported details "
        f"parse_ok={int(stats.get('counts', {}).get('detail_parse_ok', 0) or 0)} "
        f"blocked={int(stats.get('counts', {}).get('detail_blocked', 0) or 0)}"
    )
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
