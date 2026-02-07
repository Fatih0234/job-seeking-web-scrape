from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path

from scripts.crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
    write_discovery_inputs,
)
def run_spider(*, crawl_run_id: str, searches: list[dict], out_jsonl: Path) -> Path:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    inputs_path = write_discovery_inputs(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

    env = os.environ.copy()
    env.setdefault("MAX_PAGES_PER_SEARCH", "50")
    env.setdefault("MAX_JOBS_DISCOVERED_PER_SEARCH", "2000")
    env.setdefault("CIRCUIT_BREAKER_BLOCKS", "3")
    env.setdefault("DUPLICATE_PAGE_LIMIT", "3")

    cmd = [
        str(Path(".venv/bin/python")),
        str(Path(".venv/bin/scrapy")),
        "crawl",
        "linkedin_discovery_paginated",
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
    cmd = [str(Path(".venv/bin/python")), "scripts/import_discovery.py", str(jsonl_path)]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out.strip())


def main() -> None:
    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    searches = load_enabled_searches()
    if not searches:
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error="No enabled search_definitions found")
        raise SystemExit("No enabled search_definitions found; run scripts/sync_search_definitions.py first")

    create_search_runs(crawl_run_id, searches)

    out_jsonl = Path("output") / f"discovery_{crawl_run_id}.jsonl"
    run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

    stats = import_results(out_jsonl)
    finish_crawl_run(crawl_run_id, status=stats.get("status", "success"), stats=stats, error=stats.get("error"))
    print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))


if __name__ == "__main__":
    main()
