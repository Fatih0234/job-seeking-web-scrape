from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from scripts.db import connect


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python scripts/import_discovery_xing.py <jsonl_path>")

    path = Path(sys.argv[1])
    pages_by_search_run: dict[str, set[int]] = defaultdict(set)
    discovered_by_search_run: dict[str, int] = defaultdict(int)
    blocked_by_search_run: dict[str, bool] = defaultdict(bool)

    crawl_run_id = None

    with connect() as conn:
        with conn.cursor() as cur:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                rtype = rec.get("record_type")
                crawl_run_id = crawl_run_id or rec.get("crawl_run_id")

                if rtype == "page_fetch":
                    srid = rec.get("search_run_id")
                    if srid:
                        pages_by_search_run[srid].add(int(rec.get("page_start", 0)))
                        if rec.get("blocked"):
                            blocked_by_search_run[srid] = True
                    continue

                if rtype != "job_discovered":
                    continue

                job_id = rec["job_id"]
                job_url = rec["job_url"]
                srid = rec.get("search_run_id")
                if srid:
                    discovered_by_search_run[srid] += 1
                    pages_by_search_run[srid].add(int(rec.get("page_start", 0)))

                scraped_at = parse_ts(rec["scraped_at"])
                is_external = bool(rec.get("is_external"))
                list_preview = rec.get("list_preview") or {}

                cur.execute(
                    """
                    insert into job_scrape.xing_jobs
                      (
                        job_id, job_url, is_external, list_preview, first_seen_at, last_seen_at, last_seen_search_run_id,
                        is_active, stale_since_at, expired_at, expire_reason
                      )
                    values (%s, %s, %s, %s::jsonb, %s, %s, %s, true, null, null, null)
                    on conflict (job_id) do update set
                      job_url = excluded.job_url,
                      is_external = excluded.is_external,
                      list_preview = case
                        when coalesce(job_scrape.xing_jobs.list_preview, '{}'::jsonb) = '{}'::jsonb
                          then excluded.list_preview
                        else job_scrape.xing_jobs.list_preview || excluded.list_preview
                      end,
                      last_seen_at = excluded.last_seen_at,
                      last_seen_search_run_id = excluded.last_seen_search_run_id,
                      is_active = true,
                      stale_since_at = null,
                      expired_at = null,
                      expire_reason = null
                    """,
                    (job_id, job_url, is_external, json.dumps(list_preview), scraped_at, scraped_at, srid),
                )

                if srid:
                    cur.execute(
                        """
                        insert into job_scrape.xing_job_search_hits (search_run_id, job_id, rank, page_start, scraped_at)
                        values (%s, %s, %s, %s, %s)
                        on conflict do nothing
                        """,
                        (srid, job_id, int(rec.get("rank", 0)), int(rec.get("page_start", 0)), scraped_at),
                    )

            for srid, pages in pages_by_search_run.items():
                cur.execute(
                    """
                    update job_scrape.xing_search_runs
                       set finished_at = now(),
                           status = %s,
                           pages_fetched = %s,
                           jobs_discovered = %s,
                           blocked = %s
                     where id = %s
                    """,
                    (
                        "blocked" if blocked_by_search_run.get(srid) else "success",
                        len(pages),
                        discovered_by_search_run.get(srid, 0),
                        blocked_by_search_run.get(srid, False),
                        srid,
                    ),
                )

        conn.commit()

    status = "success"
    if any(blocked_by_search_run.values()):
        status = "blocked"

    out = {
        "status": status,
        "crawl_run_id": crawl_run_id,
        "search_runs": {
            srid: {
                "pages_fetched": len(pages_by_search_run[srid]),
                "jobs_discovered": discovered_by_search_run.get(srid, 0),
                "blocked": blocked_by_search_run.get(srid, False),
            }
            for srid in pages_by_search_run.keys()
        },
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
