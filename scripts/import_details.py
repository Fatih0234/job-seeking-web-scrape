from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.db import connect


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python scripts/import_details.py <jsonl_path>")

    path = Path(sys.argv[1])
    counts: Counter[str] = Counter()
    crawl_run_id = None

    with connect() as conn:
        with conn.cursor() as cur:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec: dict[str, Any] = json.loads(line)
                if rec.get("record_type") != "job_detail":
                    continue

                crawl_run_id = crawl_run_id or rec.get("crawl_run_id")

                source = rec.get("source") or "linkedin"
                job_id = rec.get("job_id")
                if not job_id:
                    counts["skipped_missing_job_id"] += 1
                    continue

                scraped_at = parse_ts(rec["scraped_at"])
                parse_ok = bool(rec.get("parse_ok"))
                blocked = bool(rec.get("blocked"))

                criteria = rec.get("criteria") or {}
                if not isinstance(criteria, dict):
                    criteria = {}

                cur.execute(
                    """
                    insert into job_scrape.job_details
                      (source, job_id, scraped_at, job_title, company_name, job_location, posted_time_ago,
                       job_description, criteria, parse_ok, last_error)
                    values
                      (%s, %s, %s, %s, %s, %s, %s,
                       %s, %s::jsonb, %s, %s)
                    on conflict (source, job_id) do update set
                      scraped_at = excluded.scraped_at,
                      job_title = excluded.job_title,
                      company_name = excluded.company_name,
                      job_location = excluded.job_location,
                      posted_time_ago = excluded.posted_time_ago,
                      job_description = excluded.job_description,
                      criteria = excluded.criteria,
                      parse_ok = excluded.parse_ok,
                      last_error = excluded.last_error
                    """,
                    (
                        source,
                        job_id,
                        scraped_at,
                        rec.get("job_title"),
                        rec.get("company_name"),
                        rec.get("job_location"),
                        rec.get("posted_time_ago"),
                        rec.get("job_description"),
                        json.dumps(criteria),
                        parse_ok,
                        rec.get("last_error") or ("blocked" if blocked else None),
                    ),
                )

                counts["detail_rows_upserted"] += 1
                if parse_ok:
                    counts["detail_parse_ok"] += 1
                else:
                    counts["detail_parse_failed"] += 1
                if blocked:
                    counts["detail_blocked"] += 1

        conn.commit()

    status = "success"
    if counts.get("detail_blocked", 0) > 0:
        status = "blocked"

    out = {
        "status": status,
        "crawl_run_id": crawl_run_id,
        "counts": dict(counts),
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()

