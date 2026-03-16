from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone

from scripts.crawl_common import (
    cleanup_stale_running_crawl_runs,
    create_crawl_run,
    create_search_runs,
    fail_running_search_runs,
    finish_crawl_run,
    load_enabled_searches,
)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[run_crawl {ts}] {msg}", file=sys.stderr)


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> str:
    try:
        out = subprocess.check_output(cmd, text=True, env=env)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed (exit={e.returncode}): {' '.join(cmd)}") from e
    return out.strip()


def _derive_crawl_status(*, discovery_status: str | None, details_status: str | None) -> str:
    status = "success"
    if discovery_status == "blocked" or details_status == "blocked":
        status = "blocked"
    if discovery_status == "failed" or details_status == "failed":
        status = "failed"
    return status


def main() -> None:
    stale_minutes = int(os.getenv("STALE_RUNNING_MINUTES", "180"))
    stale_error = f"stale: still running after {stale_minutes} minutes"
    try:
        stale_ids = cleanup_stale_running_crawl_runs(
            stale_minutes=stale_minutes,
            status="abandoned",
            error=stale_error,
        )
        if stale_ids:
            _log(f"cleaned stale running crawl_runs={stale_ids}")
    except Exception as e:
        _log(f"warning: stale cleanup failed: {e}")

    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    finalized = False
    _log(f"crawl_run_id={crawl_run_id} trigger={trigger!r}")

    def _finalize_failed(reason: str) -> None:
        nonlocal finalized
        if finalized:
            return
        try:
            fail_running_search_runs(crawl_run_id, error=reason)
        except Exception as e:
            _log(f"warning: failed to mark search_runs failed for {crawl_run_id}: {e}")
        try:
            finish_crawl_run(crawl_run_id, status="failed", stats={}, error=reason)
        finally:
            finalized = True

    def _handle_term(signum, _frame):
        reason = f"terminated_by_signal_{signum}"
        _log(f"received signal={signum}; finalizing crawl_run_id={crawl_run_id} as failed")
        _finalize_failed(reason)
        raise SystemExit(128 + signum)

    prev_int = signal.getsignal(signal.SIGINT)
    prev_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _handle_term)
    signal.signal(signal.SIGTERM, _handle_term)

    try:
        run_discovery = os.getenv("RUN_DISCOVERY", "1").strip().lower() not in {"0", "false", "no"}
        run_details = os.getenv("RUN_DETAILS", "1").strip().lower() not in {"0", "false", "no"}
        _log(f"flags RUN_DISCOVERY={int(run_discovery)} RUN_DETAILS={int(run_details)}")

        if not run_discovery and not run_details:
            raise RuntimeError("Nothing to do: both RUN_DISCOVERY and RUN_DETAILS are disabled")

        # Optional: allow YAML bootstrap into DB if requested.
        if run_discovery and os.getenv("SYNC_SEARCH_DEFINITIONS", "1") == "1":
            _log("syncing YAML search definitions into DB (scripts.sync_search_definitions)")
            _run([sys.executable, "-m", "scripts.sync_search_definitions"])

        if run_discovery:
            searches = load_enabled_searches()
            if not searches:
                raise RuntimeError("No enabled search_definitions found")
            _log(f"loaded {len(searches)} enabled searches")
            create_search_runs(crawl_run_id, searches)

        env = os.environ.copy()
        env["CRAWL_RUN_ID"] = crawl_run_id

        discovery_stats: dict = {"status": "skipped"}
        details_stats: dict = {"status": "skipped"}

        if run_discovery:
            _log("running discovery (scripts.run_discovery)")
            discovery_out = _run([sys.executable, "-m", "scripts.run_discovery"], env=env)
            discovery_stats = json.loads(discovery_out)
            _log(f"discovery done status={discovery_stats.get('status')!r}")

        if run_details:
            _log("running details (scripts.run_details)")
            details_out = _run([sys.executable, "-m", "scripts.run_details"], env=env)
            details_stats = json.loads(details_out)
            _log(f"details done status={details_stats.get('status')!r}")

        status = _derive_crawl_status(
            discovery_status=discovery_stats.get("status"),
            details_status=details_stats.get("status"),
        )

        stats = {
            "discovery": {k: v for k, v in discovery_stats.items() if k != "crawl_run_id"},
            "details": {k: v for k, v in details_stats.items() if k != "crawl_run_id"},
        }
        finish_crawl_run(crawl_run_id, status=status, stats=stats, error=None)
        finalized = True
        _log(f"finished crawl_run_id={crawl_run_id} status={status!r}")
        print(json.dumps({"crawl_run_id": crawl_run_id, "status": status, "stats": stats}, ensure_ascii=False))
    except Exception as e:
        _log(f"FAILED crawl_run_id={crawl_run_id}: {e}")
        _finalize_failed(str(e))
        raise
    finally:
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)


if __name__ == "__main__":
    main()
