from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any

from job_scrape.stepstone_config import StepstoneSearchSpec
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


def build_stepstone_facets(search: StepstoneSearchSpec) -> dict[str, Any]:
    facets: dict[str, Any] = {
        "radius": search.radius,
        "sort": search.sort,
        "where_type": search.where_type,
        "search_origin": search.search_origin,
    }
    if search.age_days is not None:
        facets["age_days"] = search.age_days
    return facets


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
    parser = argparse.ArgumentParser(description="Sync Stepstone search definitions from YAML into DB.")
    parser.add_argument(
        "--config",
        dest="config_path",
        default=os.getenv("STEPSTONE_CONFIG_PATH", "configs/stepstone.yaml"),
        help="Path to Stepstone YAML config (default: env STEPSTONE_CONFIG_PATH or configs/stepstone.yaml)",
    )
    args = parser.parse_args()

    cfg = load_stepstone_config(args.config_path)

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

                facets = build_stepstone_facets(search)

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
