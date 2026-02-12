from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.db import connect, now_utc_iso


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_details_stepstone {ts}] {msg}", file=sys.stderr)


def select_jobs_for_details(*, limit: int, staleness_days: int, blocked_retry_hours: int) -> list[dict]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.job_id, j.job_url
                  from job_scrape.stepstone_jobs j
                  left join job_scrape.stepstone_job_details d
                    on d.job_id = j.job_id
                 where (
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
                (str(staleness_days), str(blocked_retry_hours), limit),
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
    try:
        subprocess.check_call(cmd, env=env, stdout=sys.stderr, stderr=sys.stderr)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Stepstone detail spider failed (exit={e.returncode}). See Scrapy logs above.") from e

    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_details_stepstone", str(jsonl_path)]
    try:
        out = subprocess.check_output(cmd, text=True)
        return json.loads(out.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"import_details_stepstone failed (exit={e.returncode}).") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("import_details_stepstone did not return valid JSON") from e


def main() -> None:
    crawl_run_id = os.getenv("CRAWL_RUN_ID")
    if not crawl_run_id:
        raise SystemExit("CRAWL_RUN_ID env var is required (use scripts/run_crawl_stepstone.py to orchestrate)")

    limit = int(os.getenv("MAX_JOB_DETAILS_PER_RUN", "200"))
    staleness_days = int(os.getenv("DETAIL_STALENESS_DAYS", "7"))
    blocked_retry_hours = int(os.getenv("DETAIL_BLOCKED_RETRY_HOURS", "24"))
    _log(f"selecting jobs limit={limit} staleness_days={staleness_days} blocked_retry_hours={blocked_retry_hours}")

    jobs = select_jobs_for_details(limit=limit, staleness_days=staleness_days, blocked_retry_hours=blocked_retry_hours)
    if not jobs:
        _log("selected 0 jobs (nothing to do)")
        print(json.dumps({"status": "success", "crawl_run_id": crawl_run_id, "counts": {"detail_jobs_selected": 0}}))
        return

    _log(f"selected {len(jobs)} jobs for details")
    out_jsonl = Path("output") / f"stepstone_details_{crawl_run_id}.jsonl"
    run_spider(crawl_run_id=crawl_run_id, jobs=jobs, out_jsonl=out_jsonl)

    stats = import_results(out_jsonl)
    stats.setdefault("counts", {})
    stats["counts"]["detail_jobs_selected"] = len(jobs)
    _log(
        "imported details "
        f"parse_ok={int(stats.get('counts', {}).get('detail_parse_ok', 0) or 0)} "
        f"blocked={int(stats.get('counts', {}).get('detail_blocked', 0) or 0)}"
    )
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
