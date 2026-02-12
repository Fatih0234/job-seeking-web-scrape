from __future__ import annotations

import json
import os

from scripts.db import connect


def _report_linkedin(cur, run_id: str, report_source: str) -> dict:
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

    cur.execute("select count(*) from job_scrape.job_details where source=%s", (report_source,))
    (job_details_total,) = cur.fetchone()
    cur.execute("select count(*) from job_scrape.job_details where source=%s and parse_ok=true", (report_source,))
    (job_details_parse_ok,) = cur.fetchone()
    cur.execute(
        "select count(*) from job_scrape.job_details where source=%s and last_error='blocked'",
        (report_source,),
    )
    (job_details_blocked,) = cur.fetchone()

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
            where source=%s
            """,
            (report_source,),
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

    return {
        "discovery": {
            "search_runs_total": int(runs_total or 0),
            "search_runs_blocked": int(runs_blocked or 0),
            "pages_fetched_total": int(pages_total or 0),
            "jobs_discovered_total": int(jobs_total or 0),
            "search_run_statuses": search_run_statuses,
        },
        "details_overall": {
            "source": report_source,
            "job_details_total": int(job_details_total or 0),
            "job_details_parse_ok": int(job_details_parse_ok or 0),
            "job_details_blocked": int(job_details_blocked or 0),
        },
        "skills_fill": skills_fill,
    }


def _report_stepstone(cur, run_id: str) -> dict:
    cur.execute(
        """
        select
          count(*) as runs_total,
          count(*) filter (where blocked = true or status = 'blocked') as runs_blocked,
          sum(coalesce(pages_fetched, 0)) as pages_fetched_total,
          sum(coalesce(jobs_discovered, 0)) as jobs_discovered_total
        from job_scrape.stepstone_search_runs
        where crawl_run_id = %s
        """,
        (run_id,),
    )
    (runs_total, runs_blocked, pages_total, jobs_total) = cur.fetchone()

    cur.execute(
        """
        select status, count(*)
          from job_scrape.stepstone_search_runs
         where crawl_run_id = %s
         group by status
         order by status
        """,
        (run_id,),
    )
    search_run_statuses = {s: int(c) for (s, c) in cur.fetchall()}

    cur.execute("select count(*) from job_scrape.stepstone_job_details")
    (job_details_total,) = cur.fetchone()
    cur.execute("select count(*) from job_scrape.stepstone_job_details where parse_ok=true")
    (job_details_parse_ok,) = cur.fetchone()
    cur.execute("select count(*) from job_scrape.stepstone_job_details where last_error='blocked'")
    (job_details_blocked,) = cur.fetchone()

    cur.execute(
        """
        select
          count(*) filter (where parse_ok=true) as parse_ok_total,
          count(*) filter (where parse_ok=true and extracted_skills is not null) as parse_ok_with_skills
        from job_scrape.stepstone_job_details
        """
    )
    (parse_ok_total, parse_ok_with_skills) = cur.fetchone()
    parse_ok_total = int(parse_ok_total or 0)
    parse_ok_with_skills = int(parse_ok_with_skills or 0)
    pct = (parse_ok_with_skills / parse_ok_total * 100.0) if parse_ok_total else None

    return {
        "discovery": {
            "search_runs_total": int(runs_total or 0),
            "search_runs_blocked": int(runs_blocked or 0),
            "pages_fetched_total": int(pages_total or 0),
            "jobs_discovered_total": int(jobs_total or 0),
            "search_run_statuses": search_run_statuses,
        },
        "details_overall": {
            "source": "stepstone",
            "job_details_total": int(job_details_total or 0),
            "job_details_parse_ok": int(job_details_parse_ok or 0),
            "job_details_blocked": int(job_details_blocked or 0),
        },
        "skills_fill": {
            "parse_ok_total": parse_ok_total,
            "parse_ok_with_skills": parse_ok_with_skills,
            "pct": pct,
        },
    }


def _report_xing(cur, run_id: str) -> dict:
    cur.execute(
        """
        select
          count(*) as runs_total,
          count(*) filter (where blocked = true or status = 'blocked') as runs_blocked,
          sum(coalesce(pages_fetched, 0)) as pages_fetched_total,
          sum(coalesce(jobs_discovered, 0)) as jobs_discovered_total
        from job_scrape.xing_search_runs
        where crawl_run_id = %s
        """,
        (run_id,),
    )
    (runs_total, runs_blocked, pages_total, jobs_total) = cur.fetchone()

    cur.execute(
        """
        select status, count(*)
          from job_scrape.xing_search_runs
         where crawl_run_id = %s
         group by status
         order by status
        """,
        (run_id,),
    )
    search_run_statuses = {s: int(c) for (s, c) in cur.fetchall()}

    cur.execute(
        """
        select
          count(*) as hits_total,
          count(distinct h.job_id) as unique_jobs_total
          from job_scrape.xing_job_search_hits h
          join job_scrape.xing_search_runs sr on sr.id = h.search_run_id
         where sr.crawl_run_id = %s
        """,
        (run_id,),
    )
    (hits_total, unique_jobs_total) = cur.fetchone()
    hits_total = int(hits_total or 0)
    unique_jobs_total = int(unique_jobs_total or 0)
    duplicates_removed_total = max(hits_total - unique_jobs_total, 0)

    cur.execute("select count(*) from job_scrape.xing_job_details")
    (job_details_total,) = cur.fetchone()
    cur.execute("select count(*) from job_scrape.xing_job_details where parse_ok=true")
    (job_details_parse_ok,) = cur.fetchone()
    cur.execute("select count(*) from job_scrape.xing_job_details where last_error='blocked'")
    (job_details_blocked,) = cur.fetchone()

    cur.execute(
        """
        select
          count(*) filter (where parse_ok=true) as parse_ok_total,
          count(*) filter (where parse_ok=true and extracted_skills is not null) as parse_ok_with_skills
        from job_scrape.xing_job_details
        """
    )
    (parse_ok_total, parse_ok_with_skills) = cur.fetchone()
    parse_ok_total = int(parse_ok_total or 0)
    parse_ok_with_skills = int(parse_ok_with_skills or 0)
    pct = (parse_ok_with_skills / parse_ok_total * 100.0) if parse_ok_total else None

    return {
        "discovery": {
            "search_runs_total": int(runs_total or 0),
            "search_runs_blocked": int(runs_blocked or 0),
            "pages_fetched_total": int(pages_total or 0),
            "jobs_discovered_total": int(jobs_total or 0),
            "hits_total": hits_total,
            "unique_jobs_total": unique_jobs_total,
            "duplicates_removed_total": duplicates_removed_total,
            "search_run_statuses": search_run_statuses,
        },
        "details_overall": {
            "source": "xing",
            "job_details_total": int(job_details_total or 0),
            "job_details_parse_ok": int(job_details_parse_ok or 0),
            "job_details_blocked": int(job_details_blocked or 0),
        },
        "skills_fill": {
            "parse_ok_total": parse_ok_total,
            "parse_ok_with_skills": parse_ok_with_skills,
            "pct": pct,
        },
    }


def main() -> None:
    report_source = (os.getenv("REPORT_SOURCE", "linkedin") or "linkedin").strip().lower()
    with connect() as conn:
        with conn.cursor() as cur:
            if report_source == "stepstone":
                cur.execute(
                    """
                    select id, trigger, status, started_at, finished_at, error, stats
                      from job_scrape.stepstone_crawl_runs
                     order by started_at desc nulls last
                     limit 1
                    """
                )
            elif report_source == "xing":
                cur.execute(
                    """
                    select id, trigger, status, started_at, finished_at, error, stats
                      from job_scrape.xing_crawl_runs
                     order by started_at desc nulls last
                     limit 1
                    """
                )
            else:
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

            if report_source == "stepstone":
                section = _report_stepstone(cur, str(run_id))
            elif report_source == "xing":
                section = _report_xing(cur, str(run_id))
            else:
                section = _report_linkedin(cur, str(run_id), report_source)

    out = {
        "latest_crawl_run": {
            "id": str(run_id),
            "trigger": trigger,
            "status": run_status,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
            "error": error,
        },
        **section,
        "stats_json": stats,
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
