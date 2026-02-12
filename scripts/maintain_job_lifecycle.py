from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts.db import connect
from scripts.ensure_lifecycle_schema import ensure_schema


@dataclass(frozen=True)
class PlatformConfig:
    platform: str
    crawl_runs_table: str
    jobs_table: str
    hits_table: str
    details_table: str
    has_source: bool


PLATFORMS: tuple[PlatformConfig, ...] = (
    PlatformConfig(
        platform="linkedin",
        crawl_runs_table="job_scrape.crawl_runs",
        jobs_table="job_scrape.jobs",
        hits_table="job_scrape.job_search_hits",
        details_table="job_scrape.job_details",
        has_source=True,
    ),
    PlatformConfig(
        platform="stepstone",
        crawl_runs_table="job_scrape.stepstone_crawl_runs",
        jobs_table="job_scrape.stepstone_jobs",
        hits_table="job_scrape.stepstone_job_search_hits",
        details_table="job_scrape.stepstone_job_details",
        has_source=False,
    ),
    PlatformConfig(
        platform="xing",
        crawl_runs_table="job_scrape.xing_crawl_runs",
        jobs_table="job_scrape.xing_jobs",
        hits_table="job_scrape.xing_job_search_hits",
        details_table="job_scrape.xing_job_details",
        has_source=False,
    ),
)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[maintain_job_lifecycle {ts}] {msg}", file=sys.stderr)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _latest_crawl_run(cur, cfg: PlatformConfig) -> tuple[Any, ...] | None:
    cur.execute(
        f"""
        select id, status, finished_at
          from {cfg.crawl_runs_table}
         where coalesce(stats->'discovery'->>'status', '') <> 'skipped'
         order by started_at desc
         limit 1
        """
    )
    return cur.fetchone()


def _decide_platform_action(
    *,
    latest_run: tuple[Any, ...] | None,
    now_utc: datetime,
    max_crawl_age_hours: int,
) -> tuple[str, str | None]:
    if latest_run is None:
        return ("skipped_no_recent_run", "No crawl_run rows found")

    (_, latest_status, latest_finished_at) = latest_run

    if latest_status != "success":
        return ("skipped_unhealthy", f"Latest crawl status is {latest_status!r}, expected 'success'")

    if latest_finished_at is None:
        return ("skipped_unhealthy", "Latest successful crawl has no finished_at timestamp")

    if latest_finished_at.tzinfo is None:
        latest_finished_at = latest_finished_at.replace(tzinfo=timezone.utc)

    max_age = timedelta(hours=max_crawl_age_hours)
    age = now_utc - latest_finished_at
    if age > max_age:
        return (
            "skipped_unhealthy",
            f"Latest successful crawl is too old ({int(age.total_seconds() // 3600)}h > {max_crawl_age_hours}h)",
        )

    return ("processed", None)


def _count_soft_expire_candidates(cur, cfg: PlatformConfig, stale_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            select count(*)
              from {cfg.jobs_table}
             where source = %s
               and coalesce(is_active, true) = true
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (cfg.platform, str(stale_after_days)),
        )
    else:
        cur.execute(
            f"""
            select count(*)
              from {cfg.jobs_table}
             where coalesce(is_active, true) = true
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (str(stale_after_days),),
        )
    row = cur.fetchone()
    return int(row[0] or 0)


def _apply_soft_expire(cur, cfg: PlatformConfig, stale_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            update {cfg.jobs_table}
               set is_active = false,
                   stale_since_at = coalesce(stale_since_at, now()),
                   expired_at = now(),
                   expire_reason = 'not_seen_window'
             where source = %s
               and coalesce(is_active, true) = true
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (cfg.platform, str(stale_after_days)),
        )
    else:
        cur.execute(
            f"""
             update {cfg.jobs_table}
                set is_active = false,
                    stale_since_at = coalesce(stale_since_at, now()),
                    expired_at = now(),
                    expire_reason = 'not_seen_window'
             where coalesce(is_active, true) = true
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (str(stale_after_days),),
        )
    return int(cur.rowcount or 0)


def _count_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            select count(*)
              from {cfg.jobs_table}
             where source = %s
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (cfg.platform, str(hard_delete_after_days)),
        )
    else:
        cur.execute(
            f"""
            select count(*)
              from {cfg.jobs_table}
             where last_seen_at < now() - (%s || ' days')::interval
            """,
            (str(hard_delete_after_days),),
        )
    row = cur.fetchone()
    return int(row[0] or 0)


def _count_hits_for_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where source = %s
                 and last_seen_at < now() - (%s || ' days')::interval
            )
            select count(*)
              from {cfg.hits_table} h
              join candidates c on c.job_id = h.job_id
             where h.source = %s
            """,
            (cfg.platform, str(hard_delete_after_days), cfg.platform),
        )
    else:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where last_seen_at < now() - (%s || ' days')::interval
            )
            select count(*)
              from {cfg.hits_table} h
              join candidates c on c.job_id = h.job_id
            """,
            (str(hard_delete_after_days),),
        )
    row = cur.fetchone()
    return int(row[0] or 0)


def _count_details_for_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where source = %s
                 and last_seen_at < now() - (%s || ' days')::interval
            )
            select count(*)
              from {cfg.details_table} d
              join candidates c on c.job_id = d.job_id
             where d.source = %s
            """,
            (cfg.platform, str(hard_delete_after_days), cfg.platform),
        )
    else:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where last_seen_at < now() - (%s || ' days')::interval
            )
            select count(*)
              from {cfg.details_table} d
              join candidates c on c.job_id = d.job_id
            """,
            (str(hard_delete_after_days),),
        )
    row = cur.fetchone()
    return int(row[0] or 0)


def _delete_hits_for_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where source = %s
                 and last_seen_at < now() - (%s || ' days')::interval
            )
            delete from {cfg.hits_table} h
             using candidates c
             where h.source = %s
               and h.job_id = c.job_id
            """,
            (cfg.platform, str(hard_delete_after_days), cfg.platform),
        )
    else:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where last_seen_at < now() - (%s || ' days')::interval
            )
            delete from {cfg.hits_table} h
             using candidates c
             where h.job_id = c.job_id
            """,
            (str(hard_delete_after_days),),
        )
    return int(cur.rowcount or 0)


def _delete_details_for_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where source = %s
                 and last_seen_at < now() - (%s || ' days')::interval
            )
            delete from {cfg.details_table} d
             using candidates c
             where d.source = %s
               and d.job_id = c.job_id
            """,
            (cfg.platform, str(hard_delete_after_days), cfg.platform),
        )
    else:
        cur.execute(
            f"""
            with candidates as (
              select job_id
                from {cfg.jobs_table}
               where last_seen_at < now() - (%s || ' days')::interval
            )
            delete from {cfg.details_table} d
             using candidates c
             where d.job_id = c.job_id
            """,
            (str(hard_delete_after_days),),
        )
    return int(cur.rowcount or 0)


def _delete_jobs_for_hard_delete_candidates(cur, cfg: PlatformConfig, hard_delete_after_days: int) -> int:
    if cfg.has_source:
        cur.execute(
            f"""
            delete from {cfg.jobs_table}
             where source = %s
               and last_seen_at < now() - (%s || ' days')::interval
            """,
            (cfg.platform, str(hard_delete_after_days)),
        )
    else:
        cur.execute(
            f"""
            delete from {cfg.jobs_table}
             where last_seen_at < now() - (%s || ' days')::interval
            """,
            (str(hard_delete_after_days),),
        )
    return int(cur.rowcount or 0)


def _process_platform(
    *,
    cur,
    cfg: PlatformConfig,
    now_utc: datetime,
    max_crawl_age_hours: int,
    stale_after_days: int,
    hard_delete_after_days: int,
    dry_run: bool,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "platform": cfg.platform,
        "action_status": "failed",
        "latest_crawl_run_id": None,
        "latest_crawl_status": None,
        "latest_crawl_finished_at": None,
        "stale_marked_count": 0,
        "hard_delete_candidate_count": 0,
        "deleted_hits_count": 0,
        "deleted_details_count": 0,
        "deleted_jobs_count": 0,
        "note": None,
    }

    latest = _latest_crawl_run(cur, cfg)
    if latest is not None:
        stats["latest_crawl_run_id"] = str(latest[0]) if latest[0] is not None else None
        stats["latest_crawl_status"] = str(latest[1]) if latest[1] is not None else None
        stats["latest_crawl_finished_at"] = latest[2].isoformat() if latest[2] is not None else None

    action_status, note = _decide_platform_action(
        latest_run=latest,
        now_utc=now_utc,
        max_crawl_age_hours=max_crawl_age_hours,
    )
    stats["action_status"] = action_status
    stats["note"] = note

    if action_status != "processed":
        return stats

    if dry_run:
        stats["stale_marked_count"] = _count_soft_expire_candidates(cur, cfg, stale_after_days)
        stats["hard_delete_candidate_count"] = _count_hard_delete_candidates(cur, cfg, hard_delete_after_days)
        stats["deleted_hits_count"] = _count_hits_for_hard_delete_candidates(cur, cfg, hard_delete_after_days)
        stats["deleted_details_count"] = _count_details_for_hard_delete_candidates(cur, cfg, hard_delete_after_days)
        stats["deleted_jobs_count"] = stats["hard_delete_candidate_count"]
        return stats

    stats["stale_marked_count"] = _apply_soft_expire(cur, cfg, stale_after_days)
    stats["hard_delete_candidate_count"] = _count_hard_delete_candidates(cur, cfg, hard_delete_after_days)
    stats["deleted_hits_count"] = _delete_hits_for_hard_delete_candidates(cur, cfg, hard_delete_after_days)
    stats["deleted_details_count"] = _delete_details_for_hard_delete_candidates(cur, cfg, hard_delete_after_days)
    stats["deleted_jobs_count"] = _delete_jobs_for_hard_delete_candidates(cur, cfg, hard_delete_after_days)
    return stats


def _failed_platform_stats(cfg: PlatformConfig, note: str) -> dict[str, Any]:
    return {
        "platform": cfg.platform,
        "action_status": "failed",
        "latest_crawl_run_id": None,
        "latest_crawl_status": None,
        "latest_crawl_finished_at": None,
        "stale_marked_count": 0,
        "hard_delete_candidate_count": 0,
        "deleted_hits_count": 0,
        "deleted_details_count": 0,
        "deleted_jobs_count": 0,
        "note": note,
    }


def _insert_run(
    *,
    cur,
    trigger: str,
    stale_after_days: int,
    hard_delete_after_days: int,
    max_crawl_age_hours: int,
    dry_run: bool,
) -> str:
    cur.execute(
        """
        insert into job_scrape.job_lifecycle_runs
          (trigger, status, stale_after_days, hard_delete_after_days, max_crawl_age_hours, dry_run)
        values (%s, 'running', %s, %s, %s, %s)
        returning id
        """,
        (trigger, stale_after_days, hard_delete_after_days, max_crawl_age_hours, dry_run),
    )
    row = cur.fetchone()
    return str(row[0])


def _insert_platform_stats(cur, run_id: str, stats: dict[str, Any]) -> None:
    cur.execute(
        """
        insert into job_scrape.job_lifecycle_platform_stats
          (
            run_id,
            platform,
            action_status,
            latest_crawl_run_id,
            latest_crawl_status,
            latest_crawl_finished_at,
            stale_marked_count,
            hard_delete_candidate_count,
            deleted_hits_count,
            deleted_details_count,
            deleted_jobs_count,
            note
          )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (run_id, platform) do update set
          action_status = excluded.action_status,
          latest_crawl_run_id = excluded.latest_crawl_run_id,
          latest_crawl_status = excluded.latest_crawl_status,
          latest_crawl_finished_at = excluded.latest_crawl_finished_at,
          stale_marked_count = excluded.stale_marked_count,
          hard_delete_candidate_count = excluded.hard_delete_candidate_count,
          deleted_hits_count = excluded.deleted_hits_count,
          deleted_details_count = excluded.deleted_details_count,
          deleted_jobs_count = excluded.deleted_jobs_count,
          note = excluded.note
        """,
        (
            run_id,
            stats["platform"],
            stats["action_status"],
            stats.get("latest_crawl_run_id"),
            stats.get("latest_crawl_status"),
            stats.get("latest_crawl_finished_at"),
            int(stats.get("stale_marked_count", 0) or 0),
            int(stats.get("hard_delete_candidate_count", 0) or 0),
            int(stats.get("deleted_hits_count", 0) or 0),
            int(stats.get("deleted_details_count", 0) or 0),
            int(stats.get("deleted_jobs_count", 0) or 0),
            stats.get("note"),
        ),
    )


def _final_status(platform_stats: list[dict[str, Any]]) -> str:
    failed = [s for s in platform_stats if s.get("action_status") == "failed"]
    if not failed:
        return "success"
    if len(failed) == len(platform_stats):
        return "failed"
    return "partial"


def _build_summary(
    *,
    trigger: str,
    dry_run: bool,
    stale_after_days: int,
    hard_delete_after_days: int,
    max_crawl_age_hours: int,
    platform_stats: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "trigger": trigger,
        "dry_run": dry_run,
        "settings": {
            "stale_after_days": stale_after_days,
            "hard_delete_after_days": hard_delete_after_days,
            "max_crawl_age_hours": max_crawl_age_hours,
        },
        "platforms": platform_stats,
        "totals": {
            "stale_marked_count": sum(int(s.get("stale_marked_count", 0) or 0) for s in platform_stats),
            "hard_delete_candidate_count": sum(int(s.get("hard_delete_candidate_count", 0) or 0) for s in platform_stats),
            "deleted_hits_count": sum(int(s.get("deleted_hits_count", 0) or 0) for s in platform_stats),
            "deleted_details_count": sum(int(s.get("deleted_details_count", 0) or 0) for s in platform_stats),
            "deleted_jobs_count": sum(int(s.get("deleted_jobs_count", 0) or 0) for s in platform_stats),
        },
    }


def _finish_run(cur, run_id: str, *, status: str, summary: dict[str, Any], error: str | None = None) -> None:
    cur.execute(
        """
        update job_scrape.job_lifecycle_runs
           set finished_at = now(),
               status = %s,
               summary = %s::jsonb,
               error = %s
         where id = %s
        """,
        (status, json.dumps(summary, ensure_ascii=False), error, run_id),
    )


def main() -> None:
    trigger = os.getenv("LIFECYCLE_TRIGGER", os.getenv("CRAWL_TRIGGER", "manual"))
    stale_after_days = int(os.getenv("LIFECYCLE_STALE_AFTER_DAYS", "60"))
    hard_delete_after_days = int(os.getenv("LIFECYCLE_HARD_DELETE_AFTER_DAYS", "120"))
    max_crawl_age_hours = int(os.getenv("LIFECYCLE_MAX_CRAWL_AGE_HOURS", "36"))
    dry_run = _bool_env("LIFECYCLE_DRY_RUN", False)

    now_utc = datetime.now(timezone.utc)
    run_id: str | None = None

    with connect() as conn:
        try:
            ensure_schema(conn)
            conn.commit()

            with conn.cursor() as cur:
                run_id = _insert_run(
                    cur=cur,
                    trigger=trigger,
                    stale_after_days=stale_after_days,
                    hard_delete_after_days=hard_delete_after_days,
                    max_crawl_age_hours=max_crawl_age_hours,
                    dry_run=dry_run,
                )
            conn.commit()
            _log(
                f"run_id={run_id} trigger={trigger!r} dry_run={int(dry_run)} "
                f"stale_after_days={stale_after_days} hard_delete_after_days={hard_delete_after_days}"
            )

            platform_stats: list[dict[str, Any]] = []
            for cfg in PLATFORMS:
                try:
                    with conn.cursor() as cur:
                        stats = _process_platform(
                            cur=cur,
                            cfg=cfg,
                            now_utc=now_utc,
                            max_crawl_age_hours=max_crawl_age_hours,
                            stale_after_days=stale_after_days,
                            hard_delete_after_days=hard_delete_after_days,
                            dry_run=dry_run,
                        )
                        _insert_platform_stats(cur, run_id, stats)
                    conn.commit()
                except Exception as platform_err:
                    conn.rollback()
                    stats = _failed_platform_stats(cfg, str(platform_err))
                    with conn.cursor() as cur:
                        _insert_platform_stats(cur, run_id, stats)
                    conn.commit()
                    _log(f"platform={cfg.platform} FAILED: {platform_err}")
                platform_stats.append(stats)
                _log(
                    f"platform={cfg.platform} action_status={stats.get('action_status')} "
                    f"stale_marked={stats.get('stale_marked_count')} deleted_jobs={stats.get('deleted_jobs_count')}"
                )

            status = _final_status(platform_stats)
            summary = _build_summary(
                trigger=trigger,
                dry_run=dry_run,
                stale_after_days=stale_after_days,
                hard_delete_after_days=hard_delete_after_days,
                max_crawl_age_hours=max_crawl_age_hours,
                platform_stats=platform_stats,
            )
            summary["status"] = status

            with conn.cursor() as cur:
                _finish_run(cur, run_id, status=status, summary=summary, error=None)
            conn.commit()

            print(json.dumps({"run_id": run_id, **summary}, ensure_ascii=False))
        except Exception as e:
            _log(f"FAILED run_id={run_id!r}: {e}")
            if run_id is not None:
                summary = {
                    "status": "failed",
                    "trigger": trigger,
                    "dry_run": dry_run,
                    "settings": {
                        "stale_after_days": stale_after_days,
                        "hard_delete_after_days": hard_delete_after_days,
                        "max_crawl_age_hours": max_crawl_age_hours,
                    },
                    "platforms": [],
                    "totals": {},
                }
                try:
                    with conn.cursor() as cur:
                        _finish_run(cur, run_id, status="failed", summary=summary, error=str(e))
                    conn.commit()
                except Exception:
                    conn.rollback()
            raise


if __name__ == "__main__":
    main()
