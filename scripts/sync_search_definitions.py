from __future__ import annotations

import json
import re
from typing import Any

import requests

from job_scrape.linkedin_facets import build_label_to_value_map, parse_facet_options, resolve_facet_values
from job_scrape.linkedin_typeahead import build_typeahead_url, pick_best_geo_hit
from job_scrape.yaml_config import load_linkedin_config
from scripts.db import connect


UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def slugify(value: str, *, max_len: int = 40) -> str:
    """
    Stable ASCII slug for naming DB search_definitions rows.
    """
    s = _NON_ALNUM_RE.sub("_", (value or "").strip()).strip("_").lower()
    if not s:
        return "x"
    return s[:max_len]


def build_search_definition_name(*, base: str, country: str, kw_idx: int, keyword: str) -> str:
    """
    Build a unique name for a single (base search, country, keyword-variant) definition.
    """
    return f"{base}__{slugify(country)}__kw{kw_idx}_{slugify(keyword)}"


def fetch_json(url: str) -> Any:
    r = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}, timeout=30)
    r.raise_for_status()
    return r.text


def resolve_country_geo_id(country_name: str) -> str:
    url = build_typeahead_url(geo_types="COUNTRY_REGION", query=country_name)
    hits = fetch_json(url)
    best = pick_best_geo_hit(hits, prefer_suffix=country_name)
    if not best or not best.id:
        raise RuntimeError(f"Could not resolve geoId for country '{country_name}'")
    return best.id


def discover_facet_label_map(*, keywords: str, location: str, geo_id: str) -> dict[str, dict[str, str]]:
    url = (
        "https://www.linkedin.com/jobs/search?"
        + requests.compat.urlencode(
            {
                "keywords": keywords,
                "location": location,
                "geoId": geo_id,
                "pageNum": "0",
                "position": "1",
            }
        )
    )
    html = fetch_html(url)
    opts = parse_facet_options(html)
    return build_label_to_value_map(opts)


def upsert_search_definition(row: dict[str, Any]) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into job_scrape.search_definitions
                  (name, source, enabled, keywords, country_name, geo_id, location_text, facets, cities_mode, cities)
                values
                  (%(name)s, %(source)s, %(enabled)s, %(keywords)s, %(country_name)s, %(geo_id)s, %(location_text)s, %(facets)s::jsonb, %(cities_mode)s, %(cities)s::jsonb)
                on conflict (name) do update set
                  enabled = excluded.enabled,
                  keywords = excluded.keywords,
                  country_name = excluded.country_name,
                  geo_id = excluded.geo_id,
                  location_text = excluded.location_text,
                  facets = excluded.facets,
                  cities_mode = excluded.cities_mode,
                  cities = excluded.cities
                """,
                {
                    **row,
                    "facets": json.dumps(row["facets"]),
                    "cities": json.dumps(row.get("cities") or []),
                },
            )
        conn.commit()


def main() -> None:
    cfg = load_linkedin_config("configs/linkedin.yaml")
    for search in cfg.searches:
        facet_keywords = search.keywords[0]
        for country in search.countries:
            geo_id = country.geo_id or resolve_country_geo_id(country.name)
            location_text = country.location or country.name

            label_map = discover_facet_label_map(keywords=facet_keywords, location=location_text, geo_id=geo_id)

            facets: dict[str, Any] = {}
            if search.filters.date_posted:
                v = resolve_facet_values(label_map, facet="f_TPR", requested_labels=[search.filters.date_posted])
                if v:
                    facets["f_TPR"] = v[0]

            jt = resolve_facet_values(label_map, facet="f_JT", requested_labels=list(search.filters.job_type))
            if jt:
                facets["f_JT"] = jt
            ex = resolve_facet_values(label_map, facet="f_E", requested_labels=list(search.filters.experience_level))
            if ex:
                facets["f_E"] = ex
            wt = resolve_facet_values(label_map, facet="f_WT", requested_labels=list(search.filters.remote))
            if wt:
                facets["f_WT"] = wt

            # f_PP is only used when cities_mode=list, and will be resolved later (v1 keeps city names).
            cities_mode = country.cities_mode
            cities = list(country.cities)

            for idx, kw in enumerate(search.keywords):
                name = build_search_definition_name(base=search.name, country=country.name, kw_idx=idx, keyword=kw)
                upsert_search_definition(
                    {
                        "name": name,
                        "source": "linkedin",
                        "enabled": True,
                        "keywords": kw,
                        "country_name": country.name,
                        "geo_id": geo_id,
                        "location_text": location_text,
                        "facets": facets,
                        "cities_mode": cities_mode,
                        "cities": cities,
                    }
                )

    print("synced_search_definitions_ok")


if __name__ == "__main__":
    main()
