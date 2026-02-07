from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy

from job_scrape.linkedin_pagination import build_see_more_url, parse_see_more_fragment
from job_scrape.runtime import budgets


def _looks_blocked(response: scrapy.http.Response) -> bool:
    if response.status in {403, 429, 999}:
        return True
    if "/checkpoint/" in response.url:
        return True
    body_l = response.text.lower()
    return (
        "security verification" in body_l
        or "verify you are a human" in body_l
        or "unusual activity" in body_l
        or "captcha" in body_l
    )


class LinkedInDiscoveryPaginatedSpider(scrapy.Spider):
    """
    Paginate LinkedIn guest search using the seeMoreJobPostings endpoint.

    Inputs:
    - inputs: path to a JSON file with {"searches": [{...}]}
      Each search object must include:
        - search_definition_id (uuid string)
        - name
        - keywords
        - location_text
        - geo_id
        - facets (object, optional)

    Output:
    - record_type=job_discovered items
    - record_type=search_run_summary items (one per search)
    """

    name = "linkedin_discovery_paginated"
    allowed_domains = ["www.linkedin.com", "linkedin.com"]
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(self, inputs: str, crawl_run_id: str = "", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.inputs_path = inputs
        self.crawl_run_id = crawl_run_id or None

        self._b = budgets()
        self._seen_by_search: dict[str, set[str]] = {}
        self._pages_fetched: dict[str, int] = {}
        self._jobs_discovered: dict[str, int] = {}
        self._dup_pages: dict[str, int] = {}
        self._block_streak: dict[str, int] = {}
        self._blocked: dict[str, bool] = {}

    async def start(self):
        p = Path(self.inputs_path)
        data = json.loads(p.read_text(encoding="utf-8"))
        searches = data.get("searches") or []
        if not searches:
            self.logger.error("No searches in inputs file: %s", self.inputs_path)
            return

        for s in searches:
            sid = str(s["search_definition_id"])
            self._seen_by_search[sid] = set()
            self._pages_fetched[sid] = 0
            self._jobs_discovered[sid] = 0
            self._dup_pages[sid] = 0
            self._block_streak[sid] = 0
            self._blocked[sid] = False

            for req in self._schedule_page(s, start=0):
                yield req

    def _schedule_page(self, s: dict[str, Any], *, start: int):
        sid = str(s["search_definition_id"])
        if self._blocked.get(sid):
            return

        if self._pages_fetched[sid] >= self._b["MAX_PAGES_PER_SEARCH"]:
            return
        if self._jobs_discovered[sid] >= self._b["MAX_JOBS_DISCOVERED_PER_SEARCH"]:
            return
        if self._dup_pages[sid] >= self._b["DUPLICATE_PAGE_LIMIT"]:
            return

        url = build_see_more_url(
            keywords=s.get("keywords", ""),
            location=s.get("location_text", ""),
            geo_id=s.get("geo_id", ""),
            start=start,
            facets=s.get("facets") or {},
        )
        yield scrapy.Request(
            url,
            callback=self.parse_page,
            cb_kwargs={"search": s, "start": start},
            dont_filter=True,
        )

    def parse_page(self, response: scrapy.http.Response, *, search: dict[str, Any], start: int):
        sid = str(search["search_definition_id"])
        self._pages_fetched[sid] += 1

        if _looks_blocked(response):
            self._block_streak[sid] += 1
            if self._block_streak[sid] >= self._b["CIRCUIT_BREAKER_BLOCKS"]:
                self._blocked[sid] = True
                self.logger.error("Blocked detected for search %s; stopping (circuit breaker).", sid)
            return

        self._block_streak[sid] = 0

        items = parse_see_more_fragment(response.text, search_url=response.url)
        if not items:
            return

        scraped_at = datetime.now(timezone.utc).isoformat()
        page_job_ids = [it["job_id"] for it in items if it.get("job_id")]
        new_count = 0

        for rank, it in enumerate(items):
            job_id = it.get("job_id")
            job_url = it.get("job_url")
            if not job_id or not job_url:
                continue

            if job_id not in self._seen_by_search[sid]:
                self._seen_by_search[sid].add(job_id)
                new_count += 1
                self._jobs_discovered[sid] += 1

            yield {
                "record_type": "job_discovered",
                "crawl_run_id": self.crawl_run_id,
                "search_definition_id": sid,
                "search_name": search.get("name"),
                "source": "linkedin",
                "job_id": job_id,
                "job_url": job_url,
                "rank": rank,
                "page_start": start,
                "scraped_at": scraped_at,
                "search_url": response.url,
            }

        if new_count == 0:
            self._dup_pages[sid] += 1
        else:
            self._dup_pages[sid] = 0

        # Next offset: LinkedIn typically returns 10 results per fragment, but use actual count.
        next_start = start + len(page_job_ids)
        yield from self._schedule_page(search, start=next_start)

    def closed(self, reason: str):
        # Scrapy does not support yielding items from closed(). The runner/importer
        # is responsible for computing per-search summaries from the output.
        return
