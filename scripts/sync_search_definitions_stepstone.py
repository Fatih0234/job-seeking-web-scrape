from __future__ import annotations

import json
import re
from typing import Any

from job_scrape.stepstone_config import load_stepstone_config
from scripts.db import connect


_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def slugify(value: str, *, max_len: int = 40) -> str:
    s = _NON_ALNUM_RE.sub("_", (value or "").strip()).strip("_").lower()
    if not s:
        return "x"
    return s[:max_len]


def build_search_definition_name(*, base: str, location: str, location_idx: int, kw_idx: int, keyword: str) -> str:
    return (
        f"{base}__loc{location_idx}_{slugify(location)}"
        f"__kw{kw_idx}_{slugify(keyword)}"
    )


def upsert_search_definition(row: dict[str, Any]) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into job_scrape.stepstone_search_definitions
                  (name, enabled, keywords, country_name, location_text, facets)
                values
                  (%(name)s, %(enabled)s, %(keywords)s, %(country_name)s, %(location_text)s, %(facets)s::jsonb)
                on conflict (name) do update set
                  enabled = excluded.enabled,
                  keywords = excluded.keywords,
                  country_name = excluded.country_name,
                  location_text = excluded.location_text,
                  facets = excluded.facets,
                  updated_at = now()
                """,
                {
                    **row,
                    "facets": json.dumps(row["facets"]),
                },
            )
        conn.commit()


def main() -> None:
    cfg = load_stepstone_config("configs/stepstone.yaml")

    for search in cfg.searches:
        for loc_idx, location in enumerate(search.locations):
            for kw_idx, kw in enumerate(search.keywords):
                name = build_search_definition_name(
                    base=search.name,
                    location=location,
                    location_idx=loc_idx,
                    kw_idx=kw_idx,
                    keyword=kw,
                )

                facets = {
                    "radius": search.radius,
                    "sort": search.sort,
                    "where_type": search.where_type,
                    "search_origin": search.search_origin,
                }

                upsert_search_definition(
                    {
                        "name": name,
                        "enabled": True,
                        "keywords": kw,
                        "country_name": search.country,
                        "location_text": location,
                        "facets": facets,
                    }
                )

    print("synced_search_definitions_stepstone_ok")


if __name__ == "__main__":
    main()
