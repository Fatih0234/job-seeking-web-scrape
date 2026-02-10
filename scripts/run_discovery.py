from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from job_scrape.tpr_policy import apply_auto_tpr_if_any_time, normalize_facets
from scripts.crawl_common import (
    create_crawl_run,
    create_search_runs,
    finish_crawl_run,
    load_enabled_searches,
    write_discovery_inputs,
)
from scripts.db import connect


def _float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _apply_discovery_tpr_policy(*, searches: list[dict[str, Any]]) -> None:
    """
    Mutates searches in-place, adjusting the facets dict based on run history and
    env-driven TPR policy.
    """
    policy = os.getenv("DISCOVERY_TPR_POLICY", "auto_if_any_time").strip().lower()
    recent_hours = _float_env("DISCOVERY_TPR_RECENT_HOURS", 30.0)
    recent_code = os.getenv("DISCOVERY_TPR_RECENT_CODE", "r86400").strip()
    fallback_code = os.getenv("DISCOVERY_TPR_FALLBACK_CODE", "r604800").strip()

    # Defaults for missing rows (e.g. a brand-new DB).
    defaults: dict[str, dict[str, Any]] = {
        str(s["search_definition_id"]): {"has_finished_history": False, "last_success_finished_at": None}
        for s in searches
        if s.get("search_definition_id")
    }

    # Compute per-search history (batched).
    ids: list[uuid.UUID] = []
    for sid in defaults.keys():
        try:
            ids.append(uuid.UUID(str(sid)))
        except Exception:
            continue

    if ids:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                      search_definition_id,
                      bool_or(finished_at is not null) as has_finished_history,
                      max(finished_at) filter (
                        where status = 'success' and blocked = false and finished_at is not null
                      ) as last_success_finished_at
                    from job_scrape.search_runs
                    where search_definition_id = any(%s)
                    group by search_definition_id
                    """,
                    (ids,),
                )
                for (sdid, has_finished, last_success) in cur.fetchall():
                    defaults[str(sdid)] = {
                        "has_finished_history": bool(has_finished),
                        "last_success_finished_at": last_success,
                    }

    now_utc = datetime.now(timezone.utc)

    for s in searches:
        sid = str(s.get("search_definition_id") or "")
        meta = defaults.get(sid, {"has_finished_history": False, "last_success_finished_at": None})
        facets = s.get("facets") or {}

        facets_norm = normalize_facets(facets)
        if policy == "auto_if_any_time":
            facets_out = apply_auto_tpr_if_any_time(
                facets=facets_norm,
                has_finished_history=bool(meta.get("has_finished_history")),
                last_success_finished_at=meta.get("last_success_finished_at"),
                now_utc=now_utc,
                recent_hours=recent_hours,
                recent_code=recent_code,
                fallback_code=fallback_code,
            )
        else:
            # "static" (or unknown): keep as-is, just normalized.
            facets_out = facets_norm

        # Avoid polluting JSON stdout; send debug to stderr.
        if os.getenv("DISCOVERY_TPR_DEBUG", ""):
            last_s = meta.get("last_success_finished_at")
            last_s_str = last_s.isoformat() if isinstance(last_s, datetime) else None
            print(
                f"[tpr] search={s.get('name')} id={sid} "
                f"has_finished_history={meta.get('has_finished_history')} "
                f"last_success_finished_at={last_s_str} f_TPR={facets_out.get('f_TPR')!r}",
                file=sys.stderr,
            )

        s["facets"] = facets_out


def run_spider(*, crawl_run_id: str, searches: list[dict], out_jsonl: Path) -> Path:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    inputs_path = write_discovery_inputs(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

    env = os.environ.copy()
    env.setdefault("MAX_PAGES_PER_SEARCH", "50")
    env.setdefault("MAX_JOBS_DISCOVERED_PER_SEARCH", "2000")
    env.setdefault("CIRCUIT_BREAKER_BLOCKS", "3")
    env.setdefault("DUPLICATE_PAGE_LIMIT", "3")

    cmd = [
        sys.executable,
        "-m",
        "scrapy",
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
    # Keep stdout clean for the JSON status line this script prints.
    subprocess.check_call(cmd, env=env, stdout=sys.stderr, stderr=sys.stderr)
    return inputs_path


def import_results(jsonl_path: Path) -> dict:
    cmd = [sys.executable, "-m", "scripts.import_discovery", str(jsonl_path)]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out.strip())


def main() -> None:
    # If orchestrated (scripts/run_crawl.py), reuse the existing crawl run id and
    # let the orchestrator finish the crawl_runs row with combined stats.
    existing_run_id = os.getenv("CRAWL_RUN_ID")
    if existing_run_id:
        crawl_run_id = existing_run_id
        searches = load_enabled_searches()
        if not searches:
            raise SystemExit("No enabled search_definitions found; run scripts/sync_search_definitions.py first")

        create_search_runs(crawl_run_id, searches)
        _apply_discovery_tpr_policy(searches=searches)

        out_jsonl = Path("output") / f"discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
        return

    # Standalone mode: create + finish crawl_runs here.
    trigger = os.getenv("CRAWL_TRIGGER", "manual")
    crawl_run_id = create_crawl_run(trigger)
    try:
        searches = load_enabled_searches()
        if not searches:
            finish_crawl_run(crawl_run_id, status="failed", stats={}, error="No enabled search_definitions found")
            raise SystemExit("No enabled search_definitions found; run scripts/sync_search_definitions.py first")

        create_search_runs(crawl_run_id, searches)
        _apply_discovery_tpr_policy(searches=searches)

        out_jsonl = Path("output") / f"discovery_{crawl_run_id}.jsonl"
        run_spider(crawl_run_id=crawl_run_id, searches=searches, out_jsonl=out_jsonl)

        stats = import_results(out_jsonl)
        finish_crawl_run(crawl_run_id, status=stats.get("status", "success"), stats=stats, error=stats.get("error"))
        print(json.dumps({"crawl_run_id": crawl_run_id, **stats}, ensure_ascii=False))
    except Exception as e:
        finish_crawl_run(crawl_run_id, status="failed", stats={}, error=str(e))
        raise


if __name__ == "__main__":
    main()
