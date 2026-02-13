from __future__ import annotations

import json

from scripts.db import connect


SQL = """
select
  platform,
  posted_time_ago,
  count(*) as row_count
from job_scrape.jobs_dashboard_v
where posted_at_source = 'unparsed_format'
group by platform, posted_time_ago
order by platform, row_count desc, posted_time_ago
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()

    out = [
        {
            "platform": platform,
            "posted_time_ago": posted_time_ago,
            "row_count": int(row_count),
        }
        for (platform, posted_time_ago, row_count) in rows
    ]
    print(json.dumps({"unparsed_formats": out}, ensure_ascii=False))


if __name__ == "__main__":
    main()
