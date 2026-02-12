from __future__ import annotations

import json
import re
from typing import Any

from job_scrape.xing_config import XingConfig, load_xing_config
from scripts.db import connect


_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def slugify(value: str, *, max_len: int = 40) -> str:
    s = _NON_ALNUM_RE.sub("_", (value or "").strip()).strip("_").lower()
    if not s:
        return "x"
    return s[:max_len]


def build_search_definition_name(
    *,
    base: str,
    location: str | None,
    location_idx: int,
    kw_idx: int,
    keyword: str,
) -> str:
    if location is None:
        return f"{base}__locall__kw{kw_idx}_{slugify(keyword)}"
    return f"{base}__loc{location_idx}_{slugify(location)}__kw{kw_idx}_{slugify(keyword)}"


def upsert_search_definition(row: dict[str, Any]) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into job_scrape.xing_search_definitions
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


def iter_search_definition_rows(cfg: XingConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for search in cfg.searches:
        city_ids = search.city_ids or {}
        if search.locations:
            for loc_idx, location in enumerate(search.locations):
                city_id = city_ids.get(location)
                facets: dict[str, Any] = {"pagination_mode": "show_more"}
                if city_id:
                    facets["city_id"] = city_id

                for kw_idx, kw in enumerate(search.keywords):
                    name = build_search_definition_name(
                        base=search.name,
                        location=location,
                        location_idx=loc_idx,
                        kw_idx=kw_idx,
                        keyword=kw,
                    )
                    rows.append(
                        {
                            "name": name,
                            "enabled": True,
                            "keywords": kw,
                            "country_name": "",
                            "location_text": location,
                            "facets": facets,
                        }
                    )
            continue

        for kw_idx, kw in enumerate(search.keywords):
            name = build_search_definition_name(
                base=search.name,
                location=None,
                location_idx=0,
                kw_idx=kw_idx,
                keyword=kw,
            )
            rows.append(
                {
                    "name": name,
                    "enabled": True,
                    "keywords": kw,
                    "country_name": "",
                    "location_text": "",
                    "facets": {"pagination_mode": "show_more"},
                }
            )
    return rows


def main() -> None:
    cfg = load_xing_config("configs/xing.yaml")
    for row in iter_search_definition_rows(cfg):
        upsert_search_definition(row)

    print("synced_search_definitions_xing_ok")


if __name__ == "__main__":
    main()
