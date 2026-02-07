from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from scripts.db import connect, now_utc_iso


def select_jobs_for_details(*, limit: int, staleness_days: int) -> list[dict]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.source, j.job_id, j.job_url
                  from job_scrape.jobs j
                  left join job_scrape.job_details d
                    on d.source = j.source and d.job_id = j.job_id
                 where j.source = 'linkedin'
                   and (
                        d.job_id is null
                        or d.scraped_at < now() - (%s || ' days')::interval
                   )
                 order by (d.job_id is null) desc, d.scraped_at asc nulls first, j.last_seen_at desc
                 limit %s
                """,
                (str(staleness_days), limit),
            )
            rows = cur.fetchall()

    return [{"source": r[0], "job_id": r[1], "job_url": r[2]} for r in rows]


def run_spider(*, crawl_run_id: str, jobs: list[dict], out_jsonl: Path) -> Path:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    inputs = {"crawl_run_id": crawl_run_id, "generated_at": now_utc_iso(), "jobs": jobs}
    inputs_path = out_jsonl.with_suffix(".inputs.json")
    inputs_path.write_text(json.dumps(inputs, ensure_ascii=False, indent=2), encoding="utf-8")

    env = os.environ.copy()
    env.setdefault("DETAIL_DEBUG_FAILURE_LIMIT", "5")
    env.setdefault("CIRCUIT_BREAKER_BLOCKS", "3")

    cmd = [
        str(Path(".venv/bin/python")),
        str(Path(".venv/bin/scrapy")),
        "crawl",
        "linkedin_job_detail_batch",
        "-a",
        f"inputs={inputs_path}",
        "-a",
        f"crawl_run_id={crawl_run_id}",
        "-O",
        str(out_jsonl),
        "-s",
        "LOG_LEVEL=INFO",
    ]
    subprocess.check_call(cmd, env=env)
    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [str(Path(".venv/bin/python")), "scripts/import_details.py", str(jsonl_path)]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out.strip())


def main() -> None:
    crawl_run_id = os.getenv("CRAWL_RUN_ID")
    if not crawl_run_id:
        raise SystemExit("CRAWL_RUN_ID env var is required (use scripts/run_crawl.py to orchestrate)")

    limit = int(os.getenv("MAX_JOB_DETAILS_PER_RUN", "200"))
    staleness_days = int(os.getenv("DETAIL_STALENESS_DAYS", "7"))

    jobs = select_jobs_for_details(limit=limit, staleness_days=staleness_days)
    if not jobs:
        print(json.dumps({"status": "success", "crawl_run_id": crawl_run_id, "counts": {"detail_jobs_selected": 0}}))
        return

    out_jsonl = Path("output") / f"details_{crawl_run_id}.jsonl"
    run_spider(crawl_run_id=crawl_run_id, jobs=jobs, out_jsonl=out_jsonl)

    stats = import_results(out_jsonl)
    stats.setdefault("counts", {})
    stats["counts"]["detail_jobs_selected"] = len(jobs)
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()

