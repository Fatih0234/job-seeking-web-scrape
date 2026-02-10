from __future__ import annotations

import json

from scripts.db import connect


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, trigger, status, started_at, finished_at, error, stats
                  from job_scrape.crawl_runs
                 order by started_at desc nulls last
                 limit 1
                """
            )
            row = cur.fetchone()
            if not row:
                print(json.dumps({"status": "no_runs"}, ensure_ascii=False))
                return

            (run_id, trigger, run_status, started_at, finished_at, error, stats) = row

            # Discovery summary for this crawl run from search_runs.
            cur.execute(
                """
                select
                  count(*) as runs_total,
                  count(*) filter (where blocked = true or status = 'blocked') as runs_blocked,
                  sum(coalesce(pages_fetched, 0)) as pages_fetched_total,
                  sum(coalesce(jobs_discovered, 0)) as jobs_discovered_total
                from job_scrape.search_runs
                where crawl_run_id = %s
                """,
                (run_id,),
            )
            (runs_total, runs_blocked, pages_total, jobs_total) = cur.fetchone()

            cur.execute(
                """
                select status, count(*)
                  from job_scrape.search_runs
                 where crawl_run_id = %s
                 group by status
                 order by status
                """,
                (run_id,),
            )
            search_run_statuses = {s: int(c) for (s, c) in cur.fetchall()}

            # Job details overall (not per-run; job_details table is latest-only).
            cur.execute("select count(*) from job_scrape.job_details where source='linkedin'")
            (job_details_total,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.job_details where source='linkedin' and parse_ok=true")
            (job_details_parse_ok,) = cur.fetchone()
            cur.execute("select count(*) from job_scrape.job_details where source='linkedin' and last_error='blocked'")
            (job_details_blocked,) = cur.fetchone()

            # Extracted skills fill rate (if columns exist).
            cur.execute(
                """
                select column_name
                  from information_schema.columns
                 where table_schema='job_scrape'
                   and table_name='job_details'
                """
            )
            cols = {r[0] for r in cur.fetchall()}
            skills_fill = None
            if "extracted_skills" in cols:
                cur.execute(
                    """
                    select
                      count(*) filter (where parse_ok=true) as parse_ok_total,
                      count(*) filter (where parse_ok=true and extracted_skills is not null) as parse_ok_with_skills
                    from job_scrape.job_details
                    where source='linkedin'
                    """
                )
                (parse_ok_total, parse_ok_with_skills) = cur.fetchone()
                parse_ok_total = int(parse_ok_total or 0)
                parse_ok_with_skills = int(parse_ok_with_skills or 0)
                pct = (parse_ok_with_skills / parse_ok_total * 100.0) if parse_ok_total else None
                skills_fill = {
                    "parse_ok_total": parse_ok_total,
                    "parse_ok_with_skills": parse_ok_with_skills,
                    "pct": pct,
                }

    out = {
        "latest_crawl_run": {
            "id": str(run_id),
            "trigger": trigger,
            "status": run_status,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
            "error": error,
        },
        "discovery": {
            "search_runs_total": int(runs_total or 0),
            "search_runs_blocked": int(runs_blocked or 0),
            "pages_fetched_total": int(pages_total or 0),
            "jobs_discovered_total": int(jobs_total or 0),
            "search_run_statuses": search_run_statuses,
        },
        "details_overall": {
            "job_details_total": int(job_details_total or 0),
            "job_details_parse_ok": int(job_details_parse_ok or 0),
            "job_details_blocked": int(job_details_blocked or 0),
        },
        "skills_fill": skills_fill,
        "stats_json": stats,
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()

