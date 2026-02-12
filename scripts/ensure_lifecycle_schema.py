from __future__ import annotations

import sys
import time
from typing import Optional

import psycopg

from scripts.db import connect


CRITICAL_STATEMENTS: tuple[str, ...] = (
    "create schema if not exists job_scrape",
    "alter table if exists job_scrape.jobs add column if not exists is_active boolean",
    "alter table if exists job_scrape.jobs add column if not exists stale_since_at timestamptz",
    "alter table if exists job_scrape.jobs add column if not exists expired_at timestamptz",
    "alter table if exists job_scrape.jobs add column if not exists expire_reason text",
    "alter table if exists job_scrape.jobs alter column is_active set default true",
    "alter table if exists job_scrape.stepstone_jobs add column if not exists is_active boolean",
    "alter table if exists job_scrape.stepstone_jobs add column if not exists stale_since_at timestamptz",
    "alter table if exists job_scrape.stepstone_jobs add column if not exists expired_at timestamptz",
    "alter table if exists job_scrape.stepstone_jobs add column if not exists expire_reason text",
    "alter table if exists job_scrape.stepstone_jobs alter column is_active set default true",
    "alter table if exists job_scrape.xing_jobs add column if not exists is_active boolean",
    "alter table if exists job_scrape.xing_jobs add column if not exists stale_since_at timestamptz",
    "alter table if exists job_scrape.xing_jobs add column if not exists expired_at timestamptz",
    "alter table if exists job_scrape.xing_jobs add column if not exists expire_reason text",
    "alter table if exists job_scrape.xing_jobs alter column is_active set default true",
    """
    create table if not exists job_scrape.job_lifecycle_runs (
      id uuid primary key default gen_random_uuid(),
      started_at timestamptz not null default now(),
      finished_at timestamptz,
      trigger text not null,
      status text not null default 'running',
      stale_after_days integer not null,
      hard_delete_after_days integer not null,
      max_crawl_age_hours integer not null,
      dry_run boolean not null default false,
      summary jsonb not null default '{}'::jsonb,
      error text
    )
    """,
    """
    create table if not exists job_scrape.job_lifecycle_platform_stats (
      run_id uuid not null references job_scrape.job_lifecycle_runs(id) on delete cascade,
      platform text not null,
      action_status text not null,
      latest_crawl_run_id uuid,
      latest_crawl_status text,
      latest_crawl_finished_at timestamptz,
      stale_marked_count integer not null default 0,
      hard_delete_candidate_count integer not null default 0,
      deleted_hits_count integer not null default 0,
      deleted_details_count integer not null default 0,
      deleted_jobs_count integer not null default 0,
      note text,
      primary key (run_id, platform)
    )
    """,
)

OPTIONAL_INDEX_BY_TABLE: tuple[tuple[str, str], ...] = (
    (
        "job_scrape.jobs",
        "create index if not exists idx_jobs_is_active_last_seen on job_scrape.jobs(is_active, last_seen_at desc)",
    ),
    (
        "job_scrape.stepstone_jobs",
        "create index if not exists idx_stepstone_jobs_is_active_last_seen on job_scrape.stepstone_jobs(is_active, last_seen_at desc)",
    ),
    (
        "job_scrape.xing_jobs",
        "create index if not exists idx_xing_jobs_is_active_last_seen on job_scrape.xing_jobs(is_active, last_seen_at desc)",
    ),
    (
        "job_scrape.job_lifecycle_runs",
        "create index if not exists idx_job_lifecycle_runs_started_at on job_scrape.job_lifecycle_runs(started_at desc)",
    ),
    (
        "job_scrape.job_lifecycle_platform_stats",
        "create index if not exists idx_job_lifecycle_platform_stats_action_status on job_scrape.job_lifecycle_platform_stats(action_status, platform)",
    ),
)


def _is_timeout_error(err: Exception) -> bool:
    return isinstance(err, psycopg.errors.QueryCanceled)


def _table_exists(cur: psycopg.Cursor, table_name: str) -> bool:
    cur.execute("select to_regclass(%s)", (table_name,))
    return cur.fetchone()[0] is not None


def _execute_with_retries(
    cur: psycopg.Cursor,
    stmt: str,
    *,
    retries: int,
    lock_timeout_ms: int,
    required: bool,
    label: str,
) -> bool:
    for attempt in range(1, retries + 1):
        cur.execute("savepoint lifecycle_schema_stmt")
        cur.execute(f"set local lock_timeout = '{int(lock_timeout_ms)}ms'")
        try:
            cur.execute(stmt)
            cur.execute("release savepoint lifecycle_schema_stmt")
            return True
        except Exception as e:
            cur.execute("rollback to savepoint lifecycle_schema_stmt")
            cur.execute("release savepoint lifecycle_schema_stmt")
            if _is_timeout_error(e) and attempt < retries:
                time.sleep(1)
                continue
            if required:
                raise
            print(
                f"[ensure_lifecycle_schema] skipped optional statement after {attempt} attempts: {label}; error={e}",
                file=sys.stderr,
            )
            return False
    if required:
        raise RuntimeError(f"failed required schema statement: {label}")
    return False


def _ensure_schema_statements(cur: psycopg.Cursor) -> None:
    critical_retries = 120
    for idx, stmt in enumerate(CRITICAL_STATEMENTS, start=1):
        _execute_with_retries(
            cur,
            stmt,
            retries=critical_retries,
            lock_timeout_ms=1000,
            required=True,
            label=f"critical_{idx}",
        )

    for table_name, stmt in OPTIONAL_INDEX_BY_TABLE:
        if not _table_exists(cur, table_name):
            continue
        _execute_with_retries(
            cur,
            stmt,
            retries=3,
            lock_timeout_ms=1000,
            required=False,
            label=table_name,
        )


def ensure_schema(conn: Optional[psycopg.Connection] = None) -> None:
    if conn is None:
        with connect() as local_conn:
            with local_conn.cursor() as cur:
                _ensure_schema_statements(cur)
            local_conn.commit()
        return

    with conn.cursor() as cur:
        _ensure_schema_statements(cur)


def main() -> None:
    ensure_schema()
    print("lifecycle_schema_ready")


if __name__ == "__main__":
    main()
