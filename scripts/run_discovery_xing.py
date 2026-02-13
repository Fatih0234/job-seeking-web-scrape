from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.xing_crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
    write_discovery_inputs,
)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_discovery_xing {ts}] {msg}", file=sys.stderr)


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
        "xing_discovery_paginated",
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
        raise RuntimeError(f"XING discovery spider failed (exit={e.returncode}). See Scrapy logs above.") from e

    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_discovery_xing", str(jsonl_path)]
    try:
        out = subprocess.check_output(cmd, text=True)
        return json.loads(out.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"import_discovery_xing failed (exit={e.returncode}).") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("import_discovery_xing did not return valid JSON") from e


def main() -> None:
    existing_run_id = os.getenv("CRAWL_RUN_ID")
    if existing_run_id:
        crawl_run_id = existing_run_id
        searches = load_enabled_searches()
        if not searches:
            raise SystemExit("No enabled xing search_definitions found; run scripts/sync_search_definitions_xing.py first")

        create_search_runs(crawl_run_id, searches)

        out_jsonl = Path("output") / f"xing_discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
        return

    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    try:
        searches = load_enabled_searches()
        if not searches:
            finish_crawl_run(crawl_run_id, status="failed", stats={}, error="No enabled xing search_definitions found")
            raise SystemExit("No enabled xing search_definitions found; run scripts/sync_search_definitions_xing.py first")

        create_search_runs(crawl_run_id, searches)

        out_jsonl = Path("output") / f"xing_discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        finish_crawl_run(crawl_run_id, status=stats.get("status", "success"), stats=stats, error=stats.get("error"))
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
    except Exception as e:
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error=str(e))
        raise


if __name__ == "__main__":
    main()
