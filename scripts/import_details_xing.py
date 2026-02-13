from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from job_scrape.skill_extraction import extract_grouped_skills, load_skill_taxonomy
from scripts.db import connect


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse_optional_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python scripts/import_details_xing.py <jsonl_path>")

    path = Path(sys.argv[1])
    counts: Counter[str] = Counter()
    crawl_run_id = None

    taxonomy = load_skill_taxonomy()

    with connect() as conn:
        with conn.cursor() as cur:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec: dict[str, Any] = json.loads(line)
                if rec.get("record_type") != "job_detail":
                    continue

                crawl_run_id = crawl_run_id or rec.get("crawl_run_id")

                job_id = rec.get("job_id")
                if not job_id:
                    counts["skipped_missing_job_id"] += 1
                    continue

                scraped_at = parse_ts(rec["scraped_at"])
                posted_at_utc = parse_optional_ts(rec.get("posted_at_utc"))
                parse_ok = bool(rec.get("parse_ok"))
                blocked = bool(rec.get("blocked"))

                criteria = rec.get("criteria") or {}
                if not isinstance(criteria, dict):
                    criteria = {}

                job_description = rec.get("job_description")

                extracted_skills = None
                extracted_version = None
                extracted_at = None
                if parse_ok and isinstance(job_description, str) and job_description.strip():
                    extracted_skills = extract_grouped_skills(job_description, taxonomy=taxonomy)
                    extracted_version = taxonomy.version
                    extracted_at = datetime.now(timezone.utc)

                cur.execute(
                    """
                    insert into job_scrape.xing_job_details
                      (job_id, scraped_at, posted_at_utc, posted_time_ago,
                       job_title, company_name, job_location,
                       employment_type, salary_range_text, work_model,
                       job_description, criteria, parse_ok, last_error,
                       extracted_skills, extracted_skills_version, extracted_skills_extracted_at)
                    values
                      (%s, %s, %s, %s,
                       %s, %s, %s,
                       %s, %s, %s,
                       %s, %s::jsonb, %s, %s,
                       %s::jsonb, %s, %s)
                    on conflict (job_id) do update set
                      scraped_at = excluded.scraped_at,
                      posted_at_utc = excluded.posted_at_utc,
                      posted_time_ago = excluded.posted_time_ago,
                      job_title = excluded.job_title,
                      company_name = excluded.company_name,
                      job_location = excluded.job_location,
                      employment_type = excluded.employment_type,
                      salary_range_text = excluded.salary_range_text,
                      work_model = excluded.work_model,
                      job_description = excluded.job_description,
                      criteria = excluded.criteria,
                      parse_ok = excluded.parse_ok,
                      last_error = excluded.last_error,
                      extracted_skills = excluded.extracted_skills,
                      extracted_skills_version = excluded.extracted_skills_version,
                      extracted_skills_extracted_at = excluded.extracted_skills_extracted_at
                    """,
                    (
                        job_id,
                        scraped_at,
                        posted_at_utc,
                        rec.get("posted_time_ago"),
                        rec.get("job_title"),
                        rec.get("company_name"),
                        rec.get("job_location"),
                        rec.get("employment_type"),
                        rec.get("salary_range_text"),
                        rec.get("work_model"),
                        job_description,
                        json.dumps(criteria),
                        parse_ok,
                        rec.get("last_error") or ("blocked" if blocked else None),
                        json.dumps(extracted_skills) if extracted_skills is not None else None,
                        extracted_version,
                        extracted_at,
                    ),
                )

                # Keep xing_jobs status in sync with detail fetch results.
                # 410 means the posting is gone; mark inactive to avoid re-scraping forever.
                last_error = (rec.get("last_error") or "").strip() or None
                http_status = None
                if isinstance(criteria, dict):
                    http_status = criteria.get("http_status")
                if last_error == "http_410" or http_status == 410:
                    cur.execute(
                        """
                        update job_scrape.xing_jobs
                           set is_active = false,
                               expired_at = coalesce(expired_at, %s),
                               expire_reason = coalesce(expire_reason, 'http_410')
                         where job_id = %s
                        """,
                        (scraped_at, job_id),
                    )
                elif parse_ok:
                    # If we successfully parsed details, consider the job active again.
                    cur.execute(
                        """
                        update job_scrape.xing_jobs
                           set is_active = true,
                               expired_at = null,
                               expire_reason = null
                         where job_id = %s
                        """,
                        (job_id,),
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
