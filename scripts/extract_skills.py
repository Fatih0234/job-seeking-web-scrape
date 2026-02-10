from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone

from job_scrape.skill_extraction import extract_grouped_skills, load_skill_taxonomy
from scripts.db import connect


def _int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def main() -> None:
    taxonomy = load_skill_taxonomy()

    limit = _int_env("SKILL_EXTRACT_LIMIT", 500)
    only_missing = os.getenv("SKILL_EXTRACT_ONLY_MISSING", "1").strip() not in {"0", "false", "no"}

    counts: Counter[str] = Counter()

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select column_name
                  from information_schema.columns
                 where table_schema = 'job_scrape'
                   and table_name = 'job_details'
                """
            )
            cols = {r[0] for r in cur.fetchall()}
            required = {"extracted_skills", "extracted_skills_version", "extracted_skills_extracted_at"}
            missing = required - cols
            if missing:
                raise SystemExit(
                    "job_scrape.job_details missing required columns: "
                    + ", ".join(sorted(missing))
                    + " (apply the SQL migration first)"
                )

            where = "where parse_ok = true and job_description is not null"
            if only_missing:
                where += " and (extracted_skills is null or extracted_skills_version is null or extracted_skills_version < %s)"
                params = (taxonomy.version, limit)
            else:
                params = (limit,)

            if only_missing:
                cur.execute(
                    f"""
                    select source, job_id, job_description
                      from job_scrape.job_details
                      {where}
                      order by scraped_at desc
                      limit %s
                    """,
                    params,
                )
            else:
                cur.execute(
                    f"""
                    select source, job_id, job_description
                      from job_scrape.job_details
                      {where}
                      order by scraped_at desc
                      limit %s
                    """,
                    params,
                )

            rows = cur.fetchall()
            now = datetime.now(timezone.utc)
            for (source, job_id, job_description) in rows:
                skills = extract_grouped_skills(job_description, taxonomy=taxonomy)
                cur.execute(
                    """
                    update job_scrape.job_details
                       set extracted_skills = %s::jsonb,
                           extracted_skills_version = %s,
                           extracted_skills_extracted_at = %s
                     where source = %s and job_id = %s
                    """,
                    (json.dumps(skills), taxonomy.version, now, source, job_id),
                )
                counts["rows_updated"] += 1

        conn.commit()

    print(
        json.dumps(
            {
                "status": "success",
                "counts": dict(counts),
                "taxonomy_version": taxonomy.version,
                "only_missing": only_missing,
                "limit": limit,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

