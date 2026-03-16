from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.db import connect

TERMINAL_STATUSES = {"success", "blocked", "failed"}
NON_SUCCESS_OUTCOMES = {"failure", "cancelled", "skipped", "timed_out"}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def _as_flag(v: str | int | bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    return str(v).strip().lower() not in {"", "0", "false", "no", "off"}


def expected_trigger_for_workflow_event(*, workflow_name: str, workflow_event: str) -> str | None:
    wf = (workflow_name or "").strip()
    ev = (workflow_event or "").strip().lower()

    mapping = {
        ("XING Crawl (Last 24 Hours)", "schedule"): "github_schedule_last24h",
        ("XING Crawl (Last 24 Hours)", "workflow_dispatch"): "github_manual_last24h",
        ("XING Details Catch-up", "schedule"): "github_schedule_xing_details",
        ("XING Details Catch-up", "workflow_dispatch"): "github_manual_xing_details",
    }
    return mapping.get((wf, ev))


def list_stale_running_run_ids(cur, *, stale_minutes: int) -> list[str]:
    cur.execute(
        """
        select id::text
          from job_scrape.xing_crawl_runs
         where status = 'running'
           and started_at < now() - (%s || ' minutes')::interval
         order by started_at asc
        """,
        (str(stale_minutes),),
    )
    return [r[0] for r in cur.fetchall()]


def repair_stale_running_runs(cur, *, run_ids: list[str], reason: str) -> dict[str, Any]:
    repaired_search_runs = 0
    repaired_crawl_runs = 0

    for run_id in run_ids:
        cur.execute(
            """
            update job_scrape.xing_search_runs
               set status = 'failed',
                   finished_at = now(),
                   error = coalesce(error, %s)
             where crawl_run_id = %s::uuid
               and status = 'running'
            returning id
            """,
            (reason, run_id),
        )
        repaired_search_runs += len(cur.fetchall())

        cur.execute(
            """
            update job_scrape.xing_crawl_runs
               set status = 'failed',
                   finished_at = now(),
                   error = coalesce(error, %s)
             where id = %s::uuid
               and status = 'running'
            returning id
            """,
            (reason, run_id),
        )
        repaired_crawl_runs += len(cur.fetchall())

    return {
        "repaired_crawl_runs": repaired_crawl_runs,
        "repaired_search_runs": repaired_search_runs,
    }


def load_crawl_run(cur, *, crawl_run_id: str | None) -> dict[str, Any] | None:
    if not crawl_run_id:
        return None
    cur.execute(
        """
        select id::text, trigger, status, started_at, finished_at, error
          from job_scrape.xing_crawl_runs
         where id::text = %s
         limit 1
        """,
        (crawl_run_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "trigger": row[1],
        "status": row[2],
        "started_at": row[3].isoformat() if row[3] else None,
        "finished_at": row[4].isoformat() if row[4] else None,
        "error": row[5],
    }


def list_lingering_running_rows(
    cur,
    *,
    window_start_utc: datetime,
    expected_trigger: str | None,
) -> list[dict[str, Any]]:
    if expected_trigger:
        cur.execute(
            """
            select id::text, trigger, status, started_at
              from job_scrape.xing_crawl_runs
             where status = 'running'
               and trigger = %s
               and started_at >= %s::timestamptz
             order by started_at asc
            """,
            (expected_trigger, window_start_utc.isoformat()),
        )
    else:
        cur.execute(
            """
            select id::text, trigger, status, started_at
              from job_scrape.xing_crawl_runs
             where status = 'running'
               and started_at >= %s::timestamptz
             order by started_at asc
            """,
            (window_start_utc.isoformat(),),
        )

    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append(
            {
                "id": row[0],
                "trigger": row[1],
                "status": row[2],
                "started_at": row[3].isoformat() if row[3] else None,
            }
        )
    return out


def evaluate_integrity(
    *,
    expected_trigger: str | None,
    run_step_outcome: str,
    crawl_run_id: str | None,
    crawl_row: dict[str, Any] | None,
    stale_after_ids: list[str],
    lingering_running_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    checks: list[dict[str, Any]] = []
    violations: list[dict[str, str]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    def add_violation(code: str, detail: str) -> None:
        violations.append({"code": code, "detail": detail})

    outcome = (run_step_outcome or "").strip().lower()

    stale_ok = len(stale_after_ids) == 0
    add_check(
        "no_stale_running_rows_after_repair",
        stale_ok,
        f"remaining_stale_count={len(stale_after_ids)}",
    )
    if not stale_ok:
        add_violation(
            "stale_rows_remain",
            "Stale running xing_crawl_runs rows remain after attempted cleanup.",
        )

    if outcome == "success":
        has_id = bool(crawl_run_id)
        add_check("success_has_crawl_run_id", has_id, f"crawl_run_id={crawl_run_id or ''}")
        if not has_id:
            add_violation(
                "missing_crawl_run_id",
                "Run step succeeded but no crawl_run_id was provided.",
            )

        found = crawl_row is not None
        add_check("success_row_exists", found, f"found_row={int(found)}")
        if not found:
            add_violation(
                "missing_crawl_row",
                "Run step succeeded but crawl_run_id was not found in job_scrape.xing_crawl_runs.",
            )

        if found:
            status = str(crawl_row.get("status") or "")
            terminal = status in TERMINAL_STATUSES
            add_check(
                "success_row_terminal_status",
                terminal,
                f"db_status={status}",
            )
            if not terminal:
                add_violation(
                    "non_terminal_crawl_row",
                    f"crawl_run_id {crawl_row.get('id')} has non-terminal status '{status}'.",
                )

            if expected_trigger:
                trigger_matches = crawl_row.get("trigger") == expected_trigger
                add_check(
                    "success_trigger_matches",
                    trigger_matches,
                    f"db_trigger={crawl_row.get('trigger')} expected={expected_trigger}",
                )
                if not trigger_matches:
                    add_violation(
                        "trigger_mismatch",
                        "crawl_run trigger does not match expected workflow trigger.",
                    )

    elif outcome in NON_SUCCESS_OUTCOMES:
        no_lingering = len(lingering_running_rows) == 0
        add_check(
            "non_success_has_no_lingering_running_rows",
            no_lingering,
            f"lingering_running_count={len(lingering_running_rows)}",
        )
        if not no_lingering:
            add_violation(
                "lingering_running_after_non_success",
                "Workflow did not succeed but running crawl rows still exist in this workflow window.",
            )
    else:
        add_check("supported_run_step_outcome", False, f"run_step_outcome={outcome}")
        add_violation(
            "unsupported_run_step_outcome",
            f"Unsupported run-step outcome '{run_step_outcome}'.",
        )

    return checks, violations


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify XING workflow run integrity against Supabase."
    )
    parser.add_argument("--workflow-event", required=True)
    parser.add_argument("--workflow-name", required=True)
    parser.add_argument("--workflow-start-utc", required=True)
    parser.add_argument("--run-step-outcome", required=True)
    parser.add_argument("--crawl-run-id", default="")
    parser.add_argument("--stale-minutes", type=int, default=180)
    parser.add_argument("--repair-stale", default="1")
    parser.add_argument("--strict", default="1")
    args = parser.parse_args()

    strict = _as_flag(args.strict)
    repair_stale = _as_flag(args.repair_stale)
    workflow_start_utc = _parse_utc(args.workflow_start_utc)
    # Allow tiny scheduler drift around the recorded workflow start.
    window_start_utc = workflow_start_utc - timedelta(minutes=5)

    expected_trigger = expected_trigger_for_workflow_event(
        workflow_name=args.workflow_name,
        workflow_event=args.workflow_event,
    )

    stale_before_ids: list[str] = []
    stale_after_ids: list[str] = []
    repaired: dict[str, Any] = {"repaired_crawl_runs": 0, "repaired_search_runs": 0}
    crawl_row: dict[str, Any] | None = None
    lingering_rows: list[dict[str, Any]] = []

    with connect() as conn:
        with conn.cursor() as cur:
            stale_before_ids = list_stale_running_run_ids(cur, stale_minutes=args.stale_minutes)
            if repair_stale and stale_before_ids:
                repaired = repair_stale_running_runs(
                    cur,
                    run_ids=stale_before_ids,
                    reason="auto_heal_by_verify_xing_workflow_run",
                )
                conn.commit()

            stale_after_ids = list_stale_running_run_ids(cur, stale_minutes=args.stale_minutes)
            crawl_row = load_crawl_run(cur, crawl_run_id=(args.crawl_run_id or "").strip() or None)
            lingering_rows = list_lingering_running_rows(
                cur,
                window_start_utc=window_start_utc,
                expected_trigger=expected_trigger,
            )

    checks, violations = evaluate_integrity(
        expected_trigger=expected_trigger,
        run_step_outcome=args.run_step_outcome,
        crawl_run_id=(args.crawl_run_id or "").strip() or None,
        crawl_row=crawl_row,
        stale_after_ids=stale_after_ids,
        lingering_running_rows=lingering_rows,
    )

    ok = len(violations) == 0
    out = {
        "generated_at_utc": _iso_now(),
        "inputs": {
            "workflow_event": args.workflow_event,
            "workflow_name": args.workflow_name,
            "workflow_start_utc": workflow_start_utc.isoformat(),
            "run_step_outcome": args.run_step_outcome,
            "crawl_run_id": (args.crawl_run_id or "").strip() or None,
            "stale_minutes": int(args.stale_minutes),
            "repair_stale": bool(repair_stale),
            "strict": bool(strict),
        },
        "expected_trigger": expected_trigger,
        "window_start_utc": window_start_utc.isoformat(),
        "stale_cleanup": {
            "before_ids": stale_before_ids,
            "after_ids": stale_after_ids,
            **repaired,
        },
        "crawl_row": crawl_row,
        "lingering_running_rows": lingering_rows,
        "checks": checks,
        "violations": violations,
        "pass": ok,
    }
    print(json.dumps(out, ensure_ascii=False))

    if strict and not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
