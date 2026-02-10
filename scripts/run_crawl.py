from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from scripts.crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
)


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> str:
    out = subprocess.check_output(cmd, text=True, env=env)
    return out.strip()


def main() -> None:
    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)

    try:
        run_discovery = os.getenv("RUN_DISCOVERY", "1").strip().lower() not in {"0", "false", "no"}
        run_details = os.getenv("RUN_DETAILS", "1").strip().lower() not in {"0", "false", "no"}

        if not run_discovery and not run_details:
            raise RuntimeError("Nothing to do: both RUN_DISCOVERY and RUN_DETAILS are disabled")

        # Optional: allow YAML bootstrap into DB if requested.
        if run_discovery and os.getenv("SYNC_SEARCH_DEFINITIONS", "1") == "1":
            _run([sys.executable, "-m", "scripts.sync_search_definitions"])

        if run_discovery:
            searches = load_enabled_searches()
            if not searches:
                raise RuntimeError("No enabled search_definitions found")
            create_search_runs(crawl_run_id, searches)

        env = os.environ.copy()
        env["CRAWL_RUN_ID"] = crawl_run_id

        discovery_stats: dict = {"status": "skipped"}
        details_stats: dict = {"status": "skipped"}

        if run_discovery:
            discovery_out = _run([sys.executable, "-m", "scripts.run_discovery"], env=env)
            discovery_stats = json.loads(discovery_out)

        if run_details:
            details_out = _run([sys.executable, "-m", "scripts.run_details"], env=env)
            details_stats = json.loads(details_out)

        status = "success"
        if discovery_stats.get("status") == "blocked" or details_stats.get("status") == "blocked":
            status = "blocked"
        if discovery_stats.get("status") == "failed" or details_stats.get("status") == "failed":
            status = "failed"

        stats = {
            "discovery": {k: v for k, v in discovery_stats.items() if k != "crawl_run_id"},
            "details": {k: v for k, v in details_stats.items() if k != "crawl_run_id"},
        }
        finish_crawl_run(crawl_run_id, status=status, stats=stats, error=None)
        print(json.dumps({"crawl_run_id": crawl_run_id, "status": status, "stats": stats}, ensure_ascii=False))
    except Exception as e:
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error=str(e))
        raise


if __name__ == "__main__":
    main()
