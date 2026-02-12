from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from scripts.crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_crawl_xing {ts}] {msg}", file=sys.stderr)


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> str:
    try:
        out = subprocess.check_output(cmd, text=True, env=env)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed (exit={e.returncode}): {' '.join(cmd)}") from e
    return out.strip()


def main() -> None:
    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    _log(f"crawl_run_id={crawl_run_id} trigger={trigger!r}")

    try:
        run_discovery = os.getenv("RUN_DISCOVERY", "1").strip().lower() not in {"0", "false", "no"}
        _log(f"flags RUN_DISCOVERY={int(run_discovery)}")

        if not run_discovery:
            raise RuntimeError("Nothing to do: RUN_DISCOVERY is disabled")

        if os.getenv("SYNC_SEARCH_DEFINITIONS_XING", "1") == "1":
            _log("syncing XING YAML search definitions into DB (scripts.sync_search_definitions_xing)")
            _run([sys.executable, "-m", "scripts.sync_search_definitions_xing"])

        searches = load_enabled_searches(source="xing")
        if not searches:
            raise RuntimeError("No enabled xing search_definitions found")
        _log(f"loaded {len(searches)} enabled searches")
        create_search_runs(crawl_run_id, searches)

        env = os.environ.copy()
        env["CRAWL_RUN_ID"] = crawl_run_id

        _log("running discovery (scripts.run_discovery_xing)")
        discovery_out = _run([sys.executable, "-m", "scripts.run_discovery_xing"], env=env)
        discovery_stats = json.loads(discovery_out)
        _log(f"discovery done status={discovery_stats.get('status')!r}")

        status = discovery_stats.get("status", "success")
        stats = {
            "discovery": {k: v for k, v in discovery_stats.items() if k != "crawl_run_id"},
        }
        finish_crawl_run(crawl_run_id, status=status, stats=stats, error=None)
        _log(f"finished crawl_run_id={crawl_run_id} status={status!r}")
        print(json.dumps({"crawl_run_id": crawl_run_id, "status": status, "stats": stats}, ensure_ascii=False))
    except Exception as e:
        _log(f"FAILED crawl_run_id={crawl_run_id}: {e}")
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error=str(e))
        raise


if __name__ == "__main__":
    main()

