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
    print(f"[run_details_xing {ts}] {msg}", file=sys.stderr)


def select_jobs_for_details(
    *, limit: int, staleness_days: int, blocked_retry_hours: int, last_seen_window_days: int
) -> list[dict]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.job_id, j.job_url, j.is_external, j.list_preview
                 from job_scrape.xing_jobs j
                  left join job_scrape.xing_job_details d
                    on d.job_id = j.job_id
                 where j.last_seen_at > now() - (%s || ' days')::interval
                   and coalesce(j.is_active, true) = true
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

    return [
        {
            "source": "xing",
            "job_id": r[0],
            "job_url": r[1],
            "is_external": bool(r[2]),
            "list_preview": r[3] or {},
        }
        for r in rows
    ]


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
        "xing_job_detail_batch",
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
        raise RuntimeError(f"XING detail spider failed (exit={e.returncode}). See Scrapy logs above.") from e

    return inputs_path


def _external_list_only_records(*, crawl_run_id: str, jobs: list[dict]) -> list[dict]:
    now_iso = datetime.now(timezone.utc).isoformat()
    out: list[dict] = []
    for j in jobs:
        preview = j.get("list_preview") or {}
        title = preview.get("job_title")
        company = preview.get("company_name")
        location = preview.get("job_location")
        posted_at = preview.get("posted_at_utc")
        posted_ago = preview.get("posted_time_ago")
        employment_type = preview.get("employment_type")
        salary_range_text = preview.get("salary_range_text")
        work_model = preview.get("work_model")
        highlights = preview.get("highlights") or []

        parse_ok = bool(title or company or location)
        out.append(
            {
                "record_type": "job_detail",
                "crawl_run_id": crawl_run_id,
                "source": "xing",
                "job_id": j.get("job_id"),
                "job_url": j.get("job_url"),
                "scraped_at": now_iso,
                "parse_ok": parse_ok,
                "blocked": False,
                "used_playwright": False,
                "last_error": None if parse_ok else "missing_list_preview_fields",
                "posted_at_utc": posted_at,
                "posted_time_ago": posted_ago,
                "job_title": title,
                "company_name": company,
                "job_location": location,
                "employment_type": employment_type,
                "salary_range_text": salary_range_text,
                "work_model": work_model,
                "job_description": None,
                "criteria": {
                    "external_ad": True,
                    "list_only_external": True,
                    "highlights": highlights,
                    "sources": {
                        "title": "search_list",
                        "company": "search_list",
                        "location": "search_list",
                        "posted_at_utc": "search_list",
                        "employment_type": "search_list",
                        "description": None,
                    },
                },
            }
        )
    return out


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _merge_jsonl(out_path: Path, parts: list[Path]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as out:
        for p in parts:
            if not p.exists():
                continue
            for line in p.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    out.write(line + "\n")


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_details_xing", str(jsonl_path)]
    try:
        out = subprocess.check_output(cmd, text=True)
        return json.loads(out.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"import_details_xing failed (exit={e.returncode}).") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("import_details_xing did not return valid JSON") from e


def main() -> None:
    crawl_run_id = os.getenv("CRAWL_RUN_ID")
    if not crawl_run_id:
        raise SystemExit("CRAWL_RUN_ID env var is required (use scripts/run_crawl_xing.py to orchestrate)")

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

    internal_jobs = [j for j in jobs if not j.get("is_external")]
    external_jobs = [j for j in jobs if j.get("is_external")]

    _log(f"selected total={len(jobs)} internal={len(internal_jobs)} external={len(external_jobs)}")

    out_jsonl = Path("output") / f"xing_details_{crawl_run_id}.jsonl"
    part_files: list[Path] = []

    if external_jobs:
        ext_jsonl = out_jsonl.with_suffix(".external.jsonl")
        _write_jsonl(ext_jsonl, _external_list_only_records(crawl_run_id=crawl_run_id, jobs=external_jobs))
        part_files.append(ext_jsonl)

    if internal_jobs:
        internal_jsonl = out_jsonl.with_suffix(".internal.jsonl")
        run_spider(crawl_run_id=crawl_run_id, jobs=internal_jobs, out_jsonl=internal_jsonl)
        part_files.append(internal_jsonl)

    _merge_jsonl(out_jsonl, part_files)

    stats = import_results(out_jsonl)
    stats.setdefault("counts", {})
    stats["counts"]["detail_jobs_selected"] = len(jobs)
    stats["counts"]["detail_jobs_internal"] = len(internal_jobs)
    stats["counts"]["detail_jobs_external"] = len(external_jobs)
    _log(
        "imported details "
        f"parse_ok={int(stats.get('counts', {}).get('detail_parse_ok', 0) or 0)} "
        f"blocked={int(stats.get('counts', {}).get('detail_blocked', 0) or 0)}"
    )
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
