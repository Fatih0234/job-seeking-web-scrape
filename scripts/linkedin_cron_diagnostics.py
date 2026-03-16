from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.db import connect


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def _gh_schedule_by_day(*, workflow: str, since_utc: datetime, limit: int) -> dict[str, dict[str, int]]:
    cmd = [
        "gh",
        "run",
        "list",
        "--workflow",
        workflow,
        "--limit",
        str(limit),
        "--json",
        "createdAt,conclusion,event",
    ]
    out = subprocess.check_output(cmd, text=True)
    rows = json.loads(out)

    by_day: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "failures": 0})
    for row in rows:
        if row.get("event") != "schedule":
            continue
        created = _parse_utc(row["createdAt"])
        if created < since_utc:
            continue
        day = created.date().isoformat()
        by_day[day]["total"] += 1
        if row.get("conclusion") == "failure":
            by_day[day]["failures"] += 1
    return dict(by_day)


def _db_snapshot(*, days: int, stale_minutes: int) -> dict[str, Any]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                  from job_scrape.crawl_runs
                 where status = 'running'
                   and started_at < now() - (%s || ' minutes')::interval
                """,
                (str(stale_minutes),),
            )
            (stale_running_crawl_runs,) = cur.fetchone()

            cur.execute(
                """
                select count(*)
                  from job_scrape.search_runs sr
                 where sr.status = 'running'
                   and exists (
                     select 1
                       from job_scrape.crawl_runs cr
                      where cr.id = sr.crawl_run_id
                        and cr.status in ('failed', 'abandoned', 'success', 'blocked')
                   )
                """
            )
            (inconsistent_running_search_runs,) = cur.fetchone()

            cur.execute(
                """
                select
                  count(*) filter (where coalesce(stats->'details'->>'status', '') = 'blocked') as blocked_runs,
                  count(*) filter (where coalesce(stats->'details'->>'status', '') = 'skipped_backoff') as skipped_backoff_runs
                from job_scrape.crawl_runs
                where trigger = 'github_schedule_details'
                  and started_at >= now() - (%s || ' days')::interval
                """,
                (str(days),),
            )
            (details_blocked_runs, details_skipped_backoff_runs) = cur.fetchone()

            cur.execute(
                """
                select
                  to_char(date_trunc('day', started_at at time zone 'utc'), 'YYYY-MM-DD') as day_utc,
                  trigger,
                  count(*) as total,
                  count(*) filter (where status = 'success') as success,
                  count(*) filter (where status = 'blocked') as blocked,
                  count(*) filter (where status = 'failed') as failed
                from job_scrape.crawl_runs
                where trigger in ('github_schedule', 'github_schedule_details')
                  and started_at >= now() - (%s || ' days')::interval
                group by 1, 2
                order by 1 asc, 2 asc
                """,
                (str(days),),
            )
            daily_rows = cur.fetchall()

    daily: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for (day_utc, trigger, total, success, blocked, failed) in daily_rows:
        daily[trigger][day_utc] = {
            "total": int(total or 0),
            "success": int(success or 0),
            "blocked": int(blocked or 0),
            "failed": int(failed or 0),
        }

    return {
        "stale_running_crawl_runs": int(stale_running_crawl_runs or 0),
        "inconsistent_running_search_runs": int(inconsistent_running_search_runs or 0),
        "details_blocked_runs": int(details_blocked_runs or 0),
        "details_skipped_backoff_runs": int(details_skipped_backoff_runs or 0),
        "daily": dict(daily),
    }


def _compare(
    *,
    gh_by_day: dict[str, dict[str, int]],
    db_by_day: dict[str, dict[str, int]],
) -> list[dict[str, int | str]]:
    out: list[dict[str, int | str]] = []
    days = sorted(set(gh_by_day) | set(db_by_day))
    for day in days:
        gh = gh_by_day.get(day, {"total": 0, "failures": 0})
        db = db_by_day.get(day, {"total": 0, "success": 0, "blocked": 0, "failed": 0})
        out.append(
            {
                "day_utc": day,
                "gh_schedule_runs": int(gh["total"]),
                "gh_failures": int(gh["failures"]),
                "db_rows": int(db["total"]),
                "db_success": int(db["success"]),
                "db_blocked": int(db["blocked"]),
                "db_failed": int(db["failed"]),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only LinkedIn cron diagnostics (GitHub Actions vs Supabase).")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    parser.add_argument("--stale-minutes", type=int, default=180, help="Stale threshold in minutes (default: 180)")
    parser.add_argument("--gh-limit-crawl", type=int, default=120, help="GitHub run history limit for LinkedIn Crawl")
    parser.add_argument("--gh-limit-details", type=int, default=400, help="GitHub run history limit for LinkedIn Details")
    args = parser.parse_args()

    since_utc = datetime.now(timezone.utc) - timedelta(days=args.days)
    db = _db_snapshot(days=args.days, stale_minutes=args.stale_minutes)

    gh: dict[str, Any] = {
        "crawl": {"available": False, "error": None, "by_day": {}},
        "details": {"available": False, "error": None, "by_day": {}},
    }
    try:
        gh["crawl"]["by_day"] = _gh_schedule_by_day(
            workflow="LinkedIn Crawl",
            since_utc=since_utc,
            limit=args.gh_limit_crawl,
        )
        gh["crawl"]["available"] = True
    except Exception as e:
        gh["crawl"]["error"] = str(e)

    try:
        gh["details"]["by_day"] = _gh_schedule_by_day(
            workflow="LinkedIn Details",
            since_utc=since_utc,
            limit=args.gh_limit_details,
        )
        gh["details"]["available"] = True
    except Exception as e:
        gh["details"]["error"] = str(e)

    db_daily = db.get("daily", {})
    comparison = {
        "crawl": _compare(
            gh_by_day=gh["crawl"]["by_day"],
            db_by_day=db_daily.get("github_schedule", {}),
        ),
        "details": _compare(
            gh_by_day=gh["details"]["by_day"],
            db_by_day=db_daily.get("github_schedule_details", {}),
        ),
    }

    print(
        json.dumps(
            {
                "generated_at_utc": _iso_now(),
                "window_days": int(args.days),
                "stale_minutes": int(args.stale_minutes),
                "db": db,
                "github": gh,
                "comparison": comparison,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
