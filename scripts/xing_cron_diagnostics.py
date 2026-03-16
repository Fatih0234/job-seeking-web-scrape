from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.db import connect

WORKFLOW_TRIGGER_MAP: dict[str, dict[str, str]] = {
    "XING Crawl (Last 24 Hours)": {
        "schedule": "github_schedule_last24h",
        "workflow_dispatch": "github_manual_last24h",
    },
    "XING Details Catch-up": {
        "schedule": "github_schedule_xing_details",
        "workflow_dispatch": "github_manual_xing_details",
    },
}

SCHEDULE_TRIGGER_TO_WORKFLOW = {
    "github_schedule_last24h": "XING Crawl (Last 24 Hours)",
    "github_schedule_xing_details": "XING Details Catch-up",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def expected_trigger_for_workflow_event(*, workflow_name: str, event: str) -> str | None:
    return WORKFLOW_TRIGGER_MAP.get(workflow_name, {}).get((event or "").strip().lower())


def gh_runs_for_workflow(*, workflow: str, limit: int) -> list[dict[str, Any]]:
    cmd = [
        "gh",
        "run",
        "list",
        "--workflow",
        workflow,
        "--limit",
        str(limit),
        "--json",
        "databaseId,createdAt,updatedAt,status,conclusion,event,url",
    ]
    rows = json.loads(subprocess.check_output(cmd, text=True))
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "workflow": workflow,
                "run_id": int(row["databaseId"]),
                "event": row.get("event"),
                "status": row.get("status"),
                "conclusion": row.get("conclusion"),
                "created_at": _parse_utc(row["createdAt"]),
                "updated_at": _parse_utc(row["updatedAt"]),
                "url": row.get("url"),
            }
        )
    return out


def gh_schedule_by_day(*, rows: list[dict[str, Any]], since_utc: datetime) -> dict[str, dict[str, int]]:
    by_day: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "success": 0, "failure": 0, "cancelled": 0}
    )
    for row in rows:
        if row.get("event") != "schedule":
            continue
        created = row["created_at"]
        if created < since_utc:
            continue
        day = created.date().isoformat()
        by_day[day]["total"] += 1
        conclusion = (row.get("conclusion") or "").lower()
        if conclusion in by_day[day]:
            by_day[day][conclusion] += 1
    return dict(by_day)


def db_snapshot(*, days: int, stale_minutes: int) -> dict[str, Any]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text, trigger, status, started_at, finished_at, error
                  from job_scrape.xing_crawl_runs
                 where started_at >= now() - (%s || ' days')::interval
                 order by started_at desc
                """,
                (str(days),),
            )
            run_rows = cur.fetchall()

            cur.execute(
                """
                select count(*)
                  from job_scrape.xing_crawl_runs
                 where status = 'running'
                   and started_at < now() - (%s || ' minutes')::interval
                """,
                (str(stale_minutes),),
            )
            (stale_running_crawl_runs,) = cur.fetchone()

            cur.execute(
                """
                select count(*)
                  from job_scrape.xing_search_runs sr
                 where sr.status = 'running'
                   and exists (
                       select 1
                         from job_scrape.xing_crawl_runs cr
                        where cr.id = sr.crawl_run_id
                          and cr.status <> 'running'
                   )
                """
            )
            (inconsistent_running_search_runs,) = cur.fetchone()

            cur.execute(
                """
                select
                  to_char(date_trunc('day', started_at at time zone 'utc'), 'YYYY-MM-DD') as day_utc,
                  trigger,
                  count(*) as total,
                  count(*) filter (where status='success') as success,
                  count(*) filter (where status='blocked') as blocked,
                  count(*) filter (where status='failed') as failed,
                  count(*) filter (where status='running') as running
                from job_scrape.xing_crawl_runs
                where started_at >= now() - (%s || ' days')::interval
                  and trigger in (
                    'github_schedule_last24h',
                    'github_schedule_xing_details',
                    'github_manual_last24h',
                    'github_manual_xing_details'
                  )
                group by 1, 2
                order by 1 asc, 2 asc
                """,
                (str(days),),
            )
            daily_rows = cur.fetchall()

    runs: list[dict[str, Any]] = []
    for row in run_rows:
        runs.append(
            {
                "id": row[0],
                "trigger": row[1],
                "status": row[2],
                "started_at": row[3].astimezone(timezone.utc),
                "finished_at": row[4].astimezone(timezone.utc) if row[4] else None,
                "error": row[5],
            }
        )

    daily: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for row in daily_rows:
        day_utc, trigger, total, success, blocked, failed, running = row
        daily[trigger][day_utc] = {
            "total": int(total or 0),
            "success": int(success or 0),
            "blocked": int(blocked or 0),
            "failed": int(failed or 0),
            "running": int(running or 0),
        }

    return {
        "runs": runs,
        "stale_running_crawl_runs": int(stale_running_crawl_runs or 0),
        "inconsistent_running_search_runs": int(inconsistent_running_search_runs or 0),
        "daily": dict(daily),
    }


def reconcile_gh_runs_to_db(
    *,
    gh_rows: list[dict[str, Any]],
    db_rows: list[dict[str, Any]],
    max_delta_seconds: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for gh in gh_rows:
        expected_trigger = expected_trigger_for_workflow_event(
            workflow_name=gh["workflow"], event=str(gh.get("event") or "")
        )
        candidates = (
            [r for r in db_rows if r["trigger"] == expected_trigger]
            if expected_trigger
            else []
        )

        best = None
        best_sec: float | None = None
        for db in candidates:
            diff = abs((db["started_at"] - gh["created_at"]).total_seconds())
            if best is None or diff < (best_sec or float("inf")):
                best = db
                best_sec = diff

        matched = best is not None and best_sec is not None and best_sec <= max_delta_seconds

        out.append(
            {
                "workflow": gh["workflow"],
                "gh_run_id": gh["run_id"],
                "gh_event": gh.get("event"),
                "gh_conclusion": gh.get("conclusion"),
                "gh_created_at": gh["created_at"].isoformat(),
                "gh_url": gh.get("url"),
                "expected_trigger": expected_trigger,
                "matched": bool(matched),
                "delta_seconds": int(best_sec) if best_sec is not None else None,
                "db_run": {
                    "id": best["id"],
                    "trigger": best["trigger"],
                    "status": best["status"],
                    "started_at": best["started_at"].isoformat(),
                    "finished_at": best["finished_at"].isoformat() if best["finished_at"] else None,
                }
                if best
                else None,
            }
        )

    return out


def collect_run_mismatches(
    *,
    gh_reconciled: list[dict[str, Any]],
    gh_rows: list[dict[str, Any]],
    db_rows: list[dict[str, Any]],
    max_delta_seconds: int,
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []

    for row in gh_reconciled:
        conclusion = (row.get("gh_conclusion") or "").lower()
        if conclusion == "success" and not row.get("matched"):
            mismatches.append(
                {
                    "code": "gh_success_without_db_row",
                    "detail": "GitHub run succeeded but no matching DB run was found.",
                    "workflow": row.get("workflow"),
                    "gh_run_id": row.get("gh_run_id"),
                    "expected_trigger": row.get("expected_trigger"),
                }
            )

        if conclusion in {"failure", "cancelled", "timed_out"}:
            db_run = row.get("db_run") or {}
            if row.get("matched") and db_run.get("status") == "running":
                mismatches.append(
                    {
                        "code": "gh_non_success_with_lingering_running_db_row",
                        "detail": "GitHub run did not succeed but matching DB run is still running.",
                        "workflow": row.get("workflow"),
                        "gh_run_id": row.get("gh_run_id"),
                        "db_run_id": db_run.get("id"),
                    }
                )

    schedule_gh_rows = [r for r in gh_rows if r.get("event") == "schedule"]
    for db in db_rows:
        workflow = SCHEDULE_TRIGGER_TO_WORKFLOW.get(db["trigger"])
        if not workflow:
            continue

        nearest: float | None = None
        for gh in schedule_gh_rows:
            if gh["workflow"] != workflow:
                continue
            diff = abs((db["started_at"] - gh["created_at"]).total_seconds())
            if nearest is None or diff < nearest:
                nearest = diff

        if nearest is None or nearest > max_delta_seconds:
            mismatches.append(
                {
                    "code": "db_schedule_without_gh_run",
                    "detail": "DB schedule run has no matching GitHub scheduled run in the match window.",
                    "workflow": workflow,
                    "db_run_id": db["id"],
                    "db_trigger": db["trigger"],
                    "db_status": db["status"],
                }
            )

    return mismatches


def _compare(
    *,
    gh_by_day: dict[str, dict[str, int]],
    db_by_day: dict[str, dict[str, int]],
) -> list[dict[str, int | str]]:
    out: list[dict[str, int | str]] = []
    days = sorted(set(gh_by_day) | set(db_by_day))
    for day in days:
        gh = gh_by_day.get(day, {"total": 0, "success": 0, "failure": 0, "cancelled": 0})
        db = db_by_day.get(day, {"total": 0, "success": 0, "blocked": 0, "failed": 0, "running": 0})
        out.append(
            {
                "day_utc": day,
                "gh_total": int(gh["total"]),
                "gh_success": int(gh["success"]),
                "gh_failure": int(gh["failure"]),
                "gh_cancelled": int(gh["cancelled"]),
                "db_total": int(db["total"]),
                "db_success": int(db["success"]),
                "db_blocked": int(db["blocked"]),
                "db_failed": int(db["failed"]),
                "db_running": int(db["running"]),
            }
        )
    return out


def _as_flag(v: str | int | bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    return str(v).strip().lower() not in {"", "0", "false", "no", "off"}


def _filter_gh_rows_since(
    *, rows: list[dict[str, Any]], since_utc: datetime
) -> list[dict[str, Any]]:
    return [r for r in rows if r["created_at"] >= since_utc]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only XING cron diagnostics (GitHub Actions vs Supabase)."
    )
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--gh-limit-crawl", type=int, default=120)
    parser.add_argument("--gh-limit-details", type=int, default=400)
    parser.add_argument("--stale-minutes", type=int, default=180)
    parser.add_argument("--strict", default="1")
    parser.add_argument("--match-window-seconds", type=int, default=1200)
    args = parser.parse_args()

    since_utc = datetime.now(timezone.utc) - timedelta(days=args.days)
    strict = _as_flag(args.strict)

    db = db_snapshot(days=args.days, stale_minutes=args.stale_minutes)

    gh: dict[str, Any] = {
        "crawl": {"available": False, "error": None, "rows": []},
        "details": {"available": False, "error": None, "rows": []},
    }

    try:
        gh["crawl"]["rows"] = gh_runs_for_workflow(
            workflow="XING Crawl (Last 24 Hours)",
            limit=args.gh_limit_crawl,
        )
        gh["crawl"]["rows"] = _filter_gh_rows_since(
            rows=gh["crawl"]["rows"], since_utc=since_utc
        )
        gh["crawl"]["available"] = True
    except Exception as e:
        gh["crawl"]["error"] = str(e)

    try:
        gh["details"]["rows"] = gh_runs_for_workflow(
            workflow="XING Details Catch-up",
            limit=args.gh_limit_details,
        )
        gh["details"]["rows"] = _filter_gh_rows_since(
            rows=gh["details"]["rows"], since_utc=since_utc
        )
        gh["details"]["available"] = True
    except Exception as e:
        gh["details"]["error"] = str(e)

    gh_rows_all = gh["crawl"]["rows"] + gh["details"]["rows"]
    reconciled = reconcile_gh_runs_to_db(
        gh_rows=gh_rows_all,
        db_rows=db["runs"],
        max_delta_seconds=args.match_window_seconds,
    )
    mismatches = collect_run_mismatches(
        gh_reconciled=reconciled,
        gh_rows=gh_rows_all,
        db_rows=db["runs"],
        max_delta_seconds=args.match_window_seconds,
    )

    comparison = {
        "crawl_schedule": _compare(
            gh_by_day=gh_schedule_by_day(rows=gh["crawl"]["rows"], since_utc=since_utc),
            db_by_day=db["daily"].get("github_schedule_last24h", {}),
        ),
        "details_schedule": _compare(
            gh_by_day=gh_schedule_by_day(rows=gh["details"]["rows"], since_utc=since_utc),
            db_by_day=db["daily"].get("github_schedule_xing_details", {}),
        ),
    }

    output = {
        "generated_at_utc": _iso_now(),
        "window_days": int(args.days),
        "stale_minutes": int(args.stale_minutes),
        "match_window_seconds": int(args.match_window_seconds),
        "strict": bool(strict),
        "db": {
            "stale_running_crawl_runs": db["stale_running_crawl_runs"],
            "inconsistent_running_search_runs": db["inconsistent_running_search_runs"],
            "daily": db["daily"],
        },
        "github": {
            "crawl": {
                "available": gh["crawl"]["available"],
                "error": gh["crawl"]["error"],
                "rows": [
                    {
                        **r,
                        "created_at": r["created_at"].isoformat(),
                        "updated_at": r["updated_at"].isoformat(),
                    }
                    for r in gh["crawl"]["rows"]
                ],
            },
            "details": {
                "available": gh["details"]["available"],
                "error": gh["details"]["error"],
                "rows": [
                    {
                        **r,
                        "created_at": r["created_at"].isoformat(),
                        "updated_at": r["updated_at"].isoformat(),
                    }
                    for r in gh["details"]["rows"]
                ],
            },
        },
        "comparison": comparison,
        "run_reconciliation": reconciled,
        "mismatches": mismatches,
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))

    if strict:
        has_gh_gap = (not gh["crawl"]["available"]) or (not gh["details"]["available"])
        has_db_integrity_issues = (
            db["stale_running_crawl_runs"] > 0
            or db["inconsistent_running_search_runs"] > 0
        )
        if has_gh_gap or has_db_integrity_issues or len(mismatches) > 0:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
