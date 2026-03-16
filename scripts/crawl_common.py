from __future__ import annotations

import json
from pathlib import Path

import psycopg

from scripts.db import connect


def create_crawl_run(trigger: str, *, conn: psycopg.Connection | None = None) -> str:
    def _do(c: psycopg.Connection) -> str:
        with c.cursor() as cur:
            cur.execute(
                "insert into job_scrape.crawl_runs (trigger, status) values (%s, 'running') returning id",
                (trigger,),
            )
            (run_id,) = cur.fetchone()
        return str(run_id)

    if conn is not None:
        return _do(conn)
    with connect() as c:
        result = _do(c)
        c.commit()
    return result


def finish_crawl_run(run_id: str, *, status: str, stats: dict, error: str | None = None, conn: psycopg.Connection | None = None) -> None:
    def _do(c: psycopg.Connection) -> None:
        with c.cursor() as cur:
            cur.execute(
                """
                update job_scrape.crawl_runs
                   set finished_at = now(),
                       status = %s,
                       stats = %s::jsonb,
                       error = %s
                 where id = %s
                """,
                (status, json.dumps(stats), error, run_id),
            )

    if conn is not None:
        _do(conn)
        return
    with connect() as c:
        _do(c)
        c.commit()


def fail_running_search_runs(crawl_run_id: str, *, error: str | None = None) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update job_scrape.search_runs
                   set status = 'failed',
                       finished_at = now(),
                       error = coalesce(error, %s)
                 where crawl_run_id = %s
                   and status = 'running'
                returning 1
                """,
                (error, crawl_run_id),
            )
            n = len(cur.fetchall())
        conn.commit()
    return n


def cleanup_stale_running_crawl_runs(
    *,
    stale_minutes: int,
    status: str = "abandoned",
    error: str = "stale watchdog cleanup",
) -> list[str]:
    if stale_minutes <= 0:
        return []

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id
                  from job_scrape.crawl_runs
                 where status = 'running'
                   and started_at < now() - (%s || ' minutes')::interval
                """,
                (str(stale_minutes),),
            )
            run_ids = [str(r[0]) for r in cur.fetchall()]

            for run_id in run_ids:
                cur.execute(
                    """
                    update job_scrape.search_runs
                       set status = 'failed',
                           finished_at = now(),
                           error = coalesce(error, %s)
                     where crawl_run_id = %s
                       and status = 'running'
                    """,
                    (error, run_id),
                )
                cur.execute(
                    """
                    update job_scrape.crawl_runs
                       set finished_at = now(),
                           status = %s,
                           error = coalesce(error, %s)
                     where id = %s
                       and status = 'running'
                    """,
                    (status, error, run_id),
                )
        conn.commit()

    return run_ids


def load_enabled_searches(*, source: str = "linkedin", conn: psycopg.Connection | None = None) -> list[dict]:
    def _do(c: psycopg.Connection) -> list[dict]:
        with c.cursor() as cur:
            cur.execute(
                """
                select id, name, keywords, country_name, geo_id, location_text, facets, cities_mode, cities
                  from job_scrape.search_definitions
                 where enabled = true and source = %s
                 order by name asc
                """,
                (source,),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for r in rows:
            (sid, name, keywords, country_name, geo_id, location_text, facets, cities_mode, cities) = r
            out.append(
                {
                    "search_definition_id": str(sid),
                    "name": name,
                    "keywords": keywords,
                    "country_name": country_name,
                    "geo_id": geo_id,
                    "location_text": location_text,
                    "facets": facets or {},
                    "cities_mode": cities_mode,
                    "cities": cities or [],
                }
            )
        return out

    if conn is not None:
        return _do(conn)
    with connect() as c:
        return _do(c)


def create_search_runs(crawl_run_id: str, searches: list[dict], *, conn: psycopg.Connection | None = None) -> None:
    def _do(c: psycopg.Connection) -> None:
        with c.cursor() as cur:
            for s in searches:
                cur.execute(
                    """
                    insert into job_scrape.search_runs (crawl_run_id, search_definition_id, status)
                    values (%s, %s, 'running')
                    on conflict (crawl_run_id, search_definition_id) do nothing
                    returning id
                    """,
                    (crawl_run_id, s["search_definition_id"]),
                )
                row = cur.fetchone()
                if row:
                    s["search_run_id"] = str(row[0])
                else:
                    cur.execute(
                        "select id from job_scrape.search_runs where crawl_run_id=%s and search_definition_id=%s",
                        (crawl_run_id, s["search_definition_id"]),
                    )
                    (rid,) = cur.fetchone()
                    s["search_run_id"] = str(rid)

    if conn is not None:
        _do(conn)
        return
    with connect() as c:
        _do(c)
        c.commit()


def write_discovery_inputs(*, crawl_run_id: str, searches: list[dict], out_jsonl: Path) -> Path:
    inputs = {"crawl_run_id": crawl_run_id, "searches": searches}
    inputs_path = out_jsonl.with_suffix(".inputs.json")
    inputs_path.write_text(json.dumps(inputs, ensure_ascii=False, indent=2), encoding="utf-8")
    return inputs_path
