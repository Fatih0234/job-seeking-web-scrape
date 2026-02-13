from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.db import connect


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_xing_details_catchup {ts}] {msg}", file=sys.stderr)


def _missing_details_count() -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                  from job_scrape.xing_jobs j
                  left join job_scrape.xing_job_details d
                    on d.job_id = j.job_id
                 where d.job_id is null
                """
            )
            (n,) = cur.fetchone()
    return int(n or 0)


def _latest_crawl_run_id() -> str:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text
                  from job_scrape.xing_crawl_runs
                 order by started_at desc nulls last
                 limit 1
                """
            )
            row = cur.fetchone()
    if not row:
        raise RuntimeError("No xing_crawl_runs found. Run scripts.run_crawl_xing first.")
    return str(row[0])


def _parse_last_json_line(output: str) -> dict[str, Any]:
    for raw in reversed(output.splitlines()):
        line = (raw or "").strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise RuntimeError("No JSON payload found in run_details_xing output")


def _recover_partial(*, crawl_run_id: str, recover_tag: str) -> int:
    parts = [
        Path("output") / f"xing_details_{crawl_run_id}.external.jsonl",
        Path("output") / f"xing_details_{crawl_run_id}.internal.jsonl",
    ]
    out_path = Path("output") / f"xing_details_{crawl_run_id}.{recover_tag}.recover.jsonl"

    kept = 0
    skipped = 0
    with out_path.open("w", encoding="utf-8") as out:
        for p in parts:
            if not p.exists():
                continue
            for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    skipped += 1
                    continue
                out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                kept += 1

    _log(f"recover_partial kept={kept} skipped={skipped} file={out_path}")
    if kept <= 0:
        return 0

    out = subprocess.check_output(
        [sys.executable, "-m", "scripts.import_details_xing", str(out_path)],
        text=True,
    )
    payload = _parse_last_json_line(out)
    _log(f"recover_partial import status={payload.get('status')} counts={payload.get('counts')}")
    return kept


def _run_single_batch(*, crawl_run_id: str, batch_size: int, timeout_seconds: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["CRAWL_RUN_ID"] = crawl_run_id
    env["MAX_JOB_DETAILS_PER_RUN"] = str(batch_size)
    cmd = [sys.executable, "-m", "scripts.run_details_xing"]
    out = subprocess.check_output(
        cmd,
        env=env,
        text=True,
        stderr=sys.stderr,
        timeout=timeout_seconds,
    )
    return _parse_last_json_line(out)


def main() -> None:
    batch_size = int(os.getenv("XING_CATCHUP_BATCH_SIZE", "100"))
    max_batches = int(os.getenv("XING_CATCHUP_MAX_BATCHES", "500"))
    no_progress_limit = int(os.getenv("XING_CATCHUP_NO_PROGRESS_LIMIT", "10"))
    batch_timeout_seconds = int(os.getenv("XING_CATCHUP_BATCH_TIMEOUT_SECONDS", "5400"))
    sleep_seconds = int(os.getenv("XING_CATCHUP_SLEEP_SECONDS", "5"))
    recover_on_failure = os.getenv("XING_CATCHUP_RECOVER_ON_FAILURE", "1").strip().lower() not in {"0", "false", "no"}
    crawl_run_id = (os.getenv("XING_CATCHUP_CRAWL_RUN_ID") or "").strip() or _latest_crawl_run_id()

    missing_initial = _missing_details_count()
    _log(
        f"start crawl_run_id={crawl_run_id} missing={missing_initial} batch_size={batch_size} "
        f"max_batches={max_batches} no_progress_limit={no_progress_limit}"
    )

    no_progress_streak = 0
    batches_run = 0
    batches_failed = 0

    while batches_run < max_batches:
        missing_before = _missing_details_count()
        if missing_before <= 0:
            break

        batches_run += 1
        _log(f"batch={batches_run} missing_before={missing_before}")

        try:
            result = _run_single_batch(
                crawl_run_id=crawl_run_id,
                batch_size=batch_size,
                timeout_seconds=batch_timeout_seconds,
            )
            _log(f"batch={batches_run} status={result.get('status')} counts={result.get('counts')}")
        except subprocess.TimeoutExpired:
            batches_failed += 1
            no_progress_streak += 1
            _log(f"batch={batches_run} timeout after {batch_timeout_seconds}s")
            if recover_on_failure:
                try:
                    _recover_partial(crawl_run_id=crawl_run_id, recover_tag=f"timeout_b{batches_run}")
                except Exception as e:
                    _log(f"batch={batches_run} recover_partial failed: {e}")
        except Exception as e:
            batches_failed += 1
            no_progress_streak += 1
            _log(f"batch={batches_run} failed: {e}")
            if recover_on_failure:
                try:
                    _recover_partial(crawl_run_id=crawl_run_id, recover_tag=f"fail_b{batches_run}")
                except Exception as re:
                    _log(f"batch={batches_run} recover_partial failed: {re}")
        else:
            missing_after = _missing_details_count()
            delta = missing_before - missing_after
            _log(f"batch={batches_run} missing_after={missing_after} delta={delta}")
            if delta <= 0:
                no_progress_streak += 1
            else:
                no_progress_streak = 0

        if no_progress_streak >= no_progress_limit:
            _log(f"stopping: no_progress_streak={no_progress_streak} reached limit={no_progress_limit}")
            break

        time.sleep(sleep_seconds)

    missing_final = _missing_details_count()
    status = "success" if missing_final == 0 else "partial"
    out = {
        "status": status,
        "crawl_run_id": crawl_run_id,
        "missing_initial": missing_initial,
        "missing_final": missing_final,
        "batches_run": batches_run,
        "batches_failed": batches_failed,
        "no_progress_streak": no_progress_streak,
        "batch_size": batch_size,
    }
    print(json.dumps(out, ensure_ascii=False))
    if status != "success":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
