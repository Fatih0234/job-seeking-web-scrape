from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import scrapy

from job_scrape.linkedin import parse_no_results_box, parse_search_results
from job_scrape.linkedin_facets import build_label_to_value_map, parse_facet_options, resolve_facet_values
from job_scrape.linkedin_typeahead import JsonFileCache, build_typeahead_url, pick_best_geo_hit
from job_scrape.yaml_config import LinkedInConfig, LinkedInSearchSpec, load_linkedin_config


class LinkedInYamlSearchSpider(scrapy.Spider):
    """
    Reads a YAML config and runs LinkedIn jobs guest search page-1 scrapes with
    dynamic geoId resolution + dynamic facet (filter) code mapping.
    """

    name = "linkedin_yaml_search"
    allowed_domains = ["www.linkedin.com", "linkedin.com", "de.linkedin.com"]

    custom_settings = {
        # LinkedIn robots.txt likely disallows this path; keep global defaults
        # conservative, but allow this spider to run.
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, config: str = "configs/linkedin.yaml", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config_path = config
        self.cfg: LinkedInConfig = load_linkedin_config(config)

        self.geo_cache = JsonFileCache("cache/linkedin_geo.json")
        self.facet_cache = JsonFileCache("cache/linkedin_facets.json")

        self._geo: dict = self.geo_cache.load()
        self._facet_maps: dict = self.facet_cache.load()

        # Pending work tracking
        self._pending_typeahead: set[str] = set()
        self._pending_facet_pages: set[str] = set()
        self._search_queue: list[tuple[LinkedInSearchSpec, dict[str, str]]] = []

    async def start(self):
        # 1) Resolve missing geo IDs (countries + optional city facets) via typeahead.
        for search in self.cfg.searches:
            for country in search.countries:
                if country.geo_id:
                    pass
                else:
                    key = f"COUNTRY_REGION::{country.name}"
                    if key not in self._geo:
                        url = build_typeahead_url(geo_types="COUNTRY_REGION", query=country.name)
                        self._pending_typeahead.add(key)
                        yield scrapy.Request(
                            url,
                            callback=self._parse_geo_typeahead,
                            cb_kwargs={
                                "cache_key": key,
                                "prefer_suffix": country.name,
                            },
                        )

                if country.cities_mode == "list" and country.cities:
                    for city in country.cities:
                        ckey = f"POPULATED_PLACE::{country.name}::{city}"
                        if ckey in self._geo:
                            continue
                        url = build_typeahead_url(geo_types="POPULATED_PLACE", query=city)
                        self._pending_typeahead.add(ckey)
                        yield scrapy.Request(
                            url,
                            callback=self._parse_geo_typeahead,
                            cb_kwargs={
                                "cache_key": ckey,
                                "prefer_suffix": country.name,
                            },
                        )

        # If we emitted requests above, wait for callbacks to schedule the rest.
        if self._pending_typeahead:
            return

        # Otherwise proceed immediately.
        for req in self._after_geo_resolution():
            yield req

    def _parse_geo_typeahead(self, response: scrapy.http.Response, *, cache_key: str, prefer_suffix: str):
        try:
            hits = json.loads(response.text)
            best = pick_best_geo_hit(hits, prefer_suffix=prefer_suffix)
            if not best or not best.id:
                raise ValueError(f"No typeahead hits for '{cache_key}'")
            self._geo[cache_key] = {"id": best.id, "displayName": best.display_name, "type": best.type}
        finally:
            self._pending_typeahead.discard(cache_key)

        if not self._pending_typeahead:
            self.geo_cache.save(self._geo)
            yield from self._after_geo_resolution()

    def _after_geo_resolution(self):
        # 2) Ensure we have facet label->code mappings for each country (fetch 1 HTML page per country).
        for search in self.cfg.searches:
            for country in search.countries:
                geo_id = country.geo_id or self._geo.get(f"COUNTRY_REGION::{country.name}", {}).get("id")
                if not geo_id:
                    raise ValueError(f"Could not resolve geoId for country '{country.name}'")

                facet_key = f"facets::{geo_id}"
                if facet_key not in self._facet_maps:
                    url = self._build_search_url(
                        keywords=search.keywords,
                        location=country.location or country.name,
                        geo_id=geo_id,
                        page_num=0,
                        facets={},  # no filters, we just want the available options
                    )
                    self._pending_facet_pages.add(facet_key)
                    yield scrapy.Request(
                        url,
                        callback=self._parse_facets_page,
                        cb_kwargs={"facet_key": facet_key, "country_geo_id": geo_id},
                        dont_filter=True,
                    )

                # Queue actual searches to run (after facets are available).
                self._search_queue.append(
                    (
                        search,
                        {
                            "country": country.name,
                            "geo_id": geo_id,
                            "location": country.location or country.name,
                            "cities_mode": country.cities_mode,
                            "cities": list(country.cities),
                        },
                    )
                )

        if self._pending_facet_pages:
            return

        yield from self._run_searches()

    def _parse_facets_page(self, response: scrapy.http.Response, *, facet_key: str, country_geo_id: str):
        try:
            opts = parse_facet_options(response.text)
            label_map = build_label_to_value_map(opts)
            # Store a compact mapping keyed by facet name.
            self._facet_maps[facet_key] = {k: v for k, v in label_map.items()}
        finally:
            self._pending_facet_pages.discard(facet_key)

        if not self._pending_facet_pages:
            self.facet_cache.save(self._facet_maps)
            yield from self._run_searches()

    def _run_searches(self):
        for search, ctx in self._search_queue:
            geo_id = ctx["geo_id"]
            facet_key = f"facets::{geo_id}"
            label_to_value = self._facet_maps.get(facet_key, {})

            facets: dict[str, Any] = {}

            # f_TPR (radio)
            if search.filters.date_posted:
                resolved = resolve_facet_values(label_to_value, facet="f_TPR", requested_labels=[search.filters.date_posted])
                if resolved:
                    facets["f_TPR"] = resolved[0]

            # f_JT / f_E / f_WT (checkboxes)
            jt = resolve_facet_values(label_to_value, facet="f_JT", requested_labels=list(search.filters.job_type))
            if jt:
                facets["f_JT"] = jt
            ex = resolve_facet_values(label_to_value, facet="f_E", requested_labels=list(search.filters.experience_level))
            if ex:
                facets["f_E"] = ex
            wt = resolve_facet_values(label_to_value, facet="f_WT", requested_labels=list(search.filters.remote))
            if wt:
                facets["f_WT"] = wt

            cities_mode = ctx.get("cities_mode", "country_only")
            cities = ctx.get("cities") or []

            if cities_mode == "list" and cities:
                for city in cities:
                    ckey = f"POPULATED_PLACE::{ctx['country']}::{city}"
                    city_id = self._geo.get(ckey, {}).get("id")
                    if not city_id:
                        raise ValueError(f"Could not resolve city id for '{city}' in '{ctx['country']}'")
                    facets_with_city = dict(facets)
                    facets_with_city["f_PP"] = city_id
                    url = self._build_search_url(
                        keywords=search.keywords,
                        location=ctx["location"],
                        geo_id=geo_id,
                        page_num=0,
                        facets=facets_with_city,
                    )
                    yield scrapy.Request(
                        url,
                        callback=self.parse_search,
                        cb_kwargs={"search_name": f"{search.name}__{city}"},
                        dont_filter=True,
                    )
            else:
                url = self._build_search_url(
                    keywords=search.keywords,
                    location=ctx["location"],
                    geo_id=geo_id,
                    page_num=0,
                    facets=facets,
                )
                yield scrapy.Request(url, callback=self.parse_search, cb_kwargs={"search_name": search.name}, dont_filter=True)

        # Avoid re-running if callbacks fire again.
        self._search_queue = []

    def _build_search_url(
        self,
        *,
        keywords: str,
        location: str,
        geo_id: str,
        page_num: int,
        facets: dict[str, Any],
    ) -> str:
        base = "https://www.linkedin.com/jobs/search"
        params: dict[str, Any] = {
            "keywords": keywords,
            "location": location,
            "geoId": geo_id,
            "pageNum": str(page_num),
            "position": "1",
        }

        # Add facets
        for k, v in facets.items():
            params[k] = v

        return f"{base}?{urlencode(params, doseq=True)}"

    def parse_search(self, response: scrapy.http.Response, *, search_name: str):
        scraped_at = datetime.now(timezone.utc).isoformat()
        items = parse_search_results(response.text, search_url=response.url)
        if not items:
            nr = parse_no_results_box(response.text)
            if nr:
                self.logger.info("No results page detected for '%s'. URL=%s", search_name, response.url)
                return

            self.logger.warning("No job cards extracted for '%s'. URL=%s", search_name, response.url)

            # Dump HTML for debugging (unknown empty: likely selector drift).
            out_dir = Path("output")
            out_dir.mkdir(parents=True, exist_ok=True)
            safe_name = "".join(c if c.isalnum() else "_" for c in search_name)[:60]
            (out_dir / f"linkedin_{safe_name}_empty.html").write_text(response.text, encoding="utf-8")

        for it in items:
            it["scraped_at"] = scraped_at
            it["search_name"] = search_name
            yield it
