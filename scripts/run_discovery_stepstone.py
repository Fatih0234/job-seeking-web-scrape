from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

from scripts.stepstone_crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
    write_discovery_inputs,
)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_discovery_stepstone {ts}] {msg}", file=sys.stderr)


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


def _apply_age_days_override(searches: list[dict]) -> None:
    """
    Override Stepstone discovery window without re-syncing DB definitions.

    Env:
    - STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE:
      - empty: no override
      - 1 or 7: force age_days to that value for all searches
      - 0: remove age_days (Any time)
    """
    raw = (os.getenv("STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE") or "").strip()
    if not raw:
        return

    try:
        n = int(raw)
    except ValueError as e:
        raise ValueError(f"Invalid STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE={raw!r} (expected int)") from e

    if n not in {0, 1, 7}:
        raise ValueError(f"Unsupported STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE={n} (expected 0, 1, or 7)")

    for s in searches:
        facets = s.get("facets") or {}
        if not isinstance(facets, dict):
            facets = {}
        if n == 0:
            facets.pop("age_days", None)
        else:
            facets["age_days"] = n
        s["facets"] = facets

    _log(f"applied STEPSTONE_DISCOVERY_AGE_DAYS_OVERRIDE={n} to searches={len(searches)}")


def run_spider(*, crawl_run_id: str, searches: list[dict], out_jsonl: Path) -> Path:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    inputs_path = write_discovery_inputs(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

    env = os.environ.copy()
    env.setdefault("MAX_PAGES_PER_SEARCH", "50")
    env.setdefault("MAX_JOBS_DISCOVERED_PER_SEARCH", "2000")
    env.setdefault("CIRCUIT_BREAKER_BLOCKS", "3")
    env.setdefault("DUPLICATE_PAGE_LIMIT", "3")

    _log(
        f"running spider searches={len(searches)} "
        f"MAX_PAGES_PER_SEARCH={env.get('MAX_PAGES_PER_SEARCH')} "
        f"MAX_JOBS_DISCOVERED_PER_SEARCH={env.get('MAX_JOBS_DISCOVERED_PER_SEARCH')}"
    )

    cmd = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "stepstone_discovery_paginated",
        "-a",
        f"inputs={inputs_path}",
        "-a",
        f"crawl_run_id={crawl_run_id}",
        "-O",
        str(out_jsonl),
        "-s",
        "LOG_LEVEL=INFO",
    ]
    spider_timeout_seconds = int(env.get("DISCOVERY_SPIDER_TIMEOUT_SECONDS", "7200"))
    progress_timeout_seconds = int(env.get("DISCOVERY_PROGRESS_TIMEOUT_SECONDS", "300"))

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
                raise RuntimeError(f"Stepstone discovery spider failed (exit={rc}). See Scrapy logs above.")
            break

        if now - started > spider_timeout_seconds:
            _stop_process_group(proc)
            raise RuntimeError(f"Stepstone discovery spider timed out after {spider_timeout_seconds}s; aborting safely.")

        if now - last_progress > progress_timeout_seconds:
            _stop_process_group(proc)
            raise RuntimeError(
                f"Stepstone discovery spider made no output progress for {progress_timeout_seconds}s; aborting safely."
            )

        time.sleep(5)

    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_discovery_stepstone", str(jsonl_path)]
    try:
        out = subprocess.check_output(cmd, text=True)
        return json.loads(out.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"import_discovery_stepstone failed (exit={e.returncode}).") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("import_discovery_stepstone did not return valid JSON") from e


def main() -> None:
    existing_run_id = os.getenv("CRAWL_RUN_ID")
    if existing_run_id:
        crawl_run_id = existing_run_id
        searches = load_enabled_searches()
        if not searches:
            raise SystemExit("No enabled stepstone search_definitions found; run scripts/sync_search_definitions_stepstone.py first")

        _apply_age_days_override(searches)
        create_search_runs(crawl_run_id, searches)

        out_jsonl = Path("output") / f"stepstone_discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
        return

    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    try:
        searches = load_enabled_searches()
        if not searches:
            finish_crawl_run(crawl_run_id, status="failed", stats={}, error="No enabled stepstone search_definitions found")
            raise SystemExit("No enabled stepstone search_definitions found; run scripts/sync_search_definitions_stepstone.py first")

        _apply_age_days_override(searches)
        create_search_runs(crawl_run_id, searches)

        out_jsonl = Path("output") / f"stepstone_discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        finish_crawl_run(crawl_run_id, status=stats.get("status", "success"), stats=stats, error=stats.get("error"))
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
    except Exception as e:
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error=str(e))
        raise


if __name__ == "__main__":
    main()
