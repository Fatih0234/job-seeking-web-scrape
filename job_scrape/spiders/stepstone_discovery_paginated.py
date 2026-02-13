from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.runtime import budgets
from job_scrape.stepstone import (
    build_search_url,
    parse_result_counters,
    parse_search_results,
    parse_section_markers,
    select_main_results,
)


def _looks_blocked(response: scrapy.http.Response) -> bool:
    if response.status in {403, 429, 503}:
        return True

    body_l = response.text.lower()
    return (
        "access denied" in body_l
        or "errors.edgesuite.net" in body_l
        or "verify you are a human" in body_l
        or "captcha" in body_l
        or "temporarily blocked" in body_l
    )


def _looks_transient_playwright_error(msg: str) -> bool:
    # Playwright can fail with transient network/protocol errors on Stepstone/WAF edges.
    m = (msg or "").lower()
    return (
        "err_http2_protocol_error" in m
        or "net::err_http2_protocol_error" in m
        or "net::err_connection_closed" in m
        or "net::err_connection_reset" in m
        or "net::err_connection_refused" in m
        or "net::err_timed_out" in m
    )


class StepstoneDiscoveryPaginatedSpider(scrapy.Spider):
    """
    Paginate Stepstone result pages and discover only `main` jobs.

    Inputs:
    - inputs: path to a JSON file with {"searches": [{...}]}
      Each search object should include:
        - search_definition_id (uuid string)
        - name
        - keywords
        - location_text
        - facets (object, optional): radius, sort, where_type, search_origin, age_days

    Output:
    - record_type=job_discovered items
    - record_type=page_fetch items
    """

    name = "stepstone_discovery_paginated"
    allowed_domains = ["www.stepstone.de", "stepstone.de"]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        # Playwright-first due to frequent Stepstone WAF 403 on plain HTTP.
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2.0,
        # Prevent a single stuck request from hanging the whole run.
        "DOWNLOAD_TIMEOUT": 60,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
    }

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

            for req in self._schedule_page(s, page_num=1):
                yield req

    def _schedule_page(self, s: dict[str, Any], *, page_num: int):
        sid = str(s["search_definition_id"])
        if self._blocked.get(sid):
            return

        if self._pages_fetched[sid] >= self._b["MAX_PAGES_PER_SEARCH"]:
            return
        if self._jobs_discovered[sid] >= self._b["MAX_JOBS_DISCOVERED_PER_SEARCH"]:
            return
        if self._dup_pages[sid] >= self._b["DUPLICATE_PAGE_LIMIT"]:
            return

        facets = s.get("facets") or {}
        radius = int(facets.get("radius", 30))
        sort = facets.get("sort", 2)
        where_type = str(facets.get("where_type", "autosuggest") or "autosuggest")
        search_origin = str(facets.get("search_origin", "Resultlist_top-search") or "Resultlist_top-search")
        age_days = facets.get("age_days")

        url = build_search_url(
            keywords=s.get("keywords", ""),
            location=s.get("location_text", ""),
            radius=radius,
            sort=sort,
            age_days=age_days,
            page=page_num,
            where_type=where_type,
            search_origin=search_origin,
        )

        yield scrapy.Request(
            url,
            callback=self.parse_page,
            errback=self.parse_error,
            cb_kwargs={"search": s, "page_num": page_num},
            dont_filter=True,
            meta={
                "search_definition_id": sid,
                "search_run_id": s.get("search_run_id"),
                "search_name": s.get("name"),
                "page_num": page_num,
                "playwright": True,
                # Avoid waiting for full "load" and set an explicit timeout.
                "playwright_page_goto_kwargs": {"timeout": 60_000, "wait_until": "domcontentloaded"},
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_timeout", 1000),
                ],
            },
        )

    async def parse_error(self, failure):
        req = failure.request
        sid = str(req.meta.get("search_definition_id") or "")
        search_run_id = req.meta.get("search_run_id")
        search_name = req.meta.get("search_name")
        page_num = int(req.meta.get("page_num") or 0) or 1

        fetched_at = datetime.now(timezone.utc).isoformat()
        page = req.meta.get("playwright_page")

        response = getattr(getattr(failure, "value", None), "response", None)
        status_code = getattr(response, "status", None)
        msg = str(getattr(failure, "value", failure) or "request_failed")

        blocked = False
        if status_code in {403, 429, 503}:
            blocked = True
        if _looks_transient_playwright_error(msg):
            # Treat as blocked-like for circuit breaker purposes: stop quickly and retry next run.
            blocked = True

        if sid:
            if blocked:
                self._block_streak[sid] = int(self._block_streak.get(sid, 0) or 0) + 1
                if self._block_streak[sid] >= self._b["CIRCUIT_BREAKER_BLOCKS"]:
                    self._blocked[sid] = True
                    self.logger.error("Request failures for search %s; stopping (circuit breaker).", sid)
            else:
                self._block_streak[sid] = 0

        try:
            yield {
                "record_type": "page_fetch",
                "crawl_run_id": self.crawl_run_id,
                "search_definition_id": sid or None,
                "search_run_id": search_run_id,
                "search_name": search_name,
                "page_start": page_num,
                "status_code": status_code,
                "blocked": bool(blocked),
                "item_count": 0,
                "fetched_at": fetched_at,
                "url": req.url,
                "error": msg[:500],
            }
        finally:
            if page:
                await page.close()

    async def parse_page(self, response: scrapy.http.Response, *, search: dict[str, Any], page_num: int):
        sid = str(search["search_definition_id"])
        search_run_id = search.get("search_run_id")
        self._pages_fetched[sid] += 1

        page = response.meta.get("playwright_page")
        fetched_at = datetime.now(timezone.utc).isoformat()

        try:
            if _looks_blocked(response):
                self._block_streak[sid] += 1
                yield {
                    "record_type": "page_fetch",
                    "crawl_run_id": self.crawl_run_id,
                    "search_definition_id": sid,
                    "search_run_id": search_run_id,
                    "search_name": search.get("name"),
                    "page_start": page_num,
                    "status_code": response.status,
                    "blocked": True,
                    "item_count": 0,
                    "fetched_at": fetched_at,
                    "url": response.url,
                }
                if self._block_streak[sid] >= self._b["CIRCUIT_BREAKER_BLOCKS"]:
                    self._blocked[sid] = True
                    self.logger.error("Blocked detected for search %s; stopping (circuit breaker).", sid)
                return

            self._block_streak[sid] = 0

            counters = parse_result_counters(response.text)
            markers = parse_section_markers(response.text)
            items = parse_search_results(response.text, search_url=response.url)
            main_items = select_main_results(items, counters=counters)

            yield {
                "record_type": "page_fetch",
                "crawl_run_id": self.crawl_run_id,
                "search_definition_id": sid,
                "search_run_id": search_run_id,
                "search_name": search.get("name"),
                "page_start": page_num,
                "status_code": response.status,
                "blocked": False,
                "item_count": len(main_items),
                "fetched_at": fetched_at,
                "url": response.url,
                "main_total": counters.main if counters else None,
                "main_displayed": counters.main_displayed if counters else None,
                "regional_displayed": counters.regional_displayed if counters else None,
                "semantic_displayed": counters.semantic_displayed if counters else None,
                "markers": markers,
            }

            if not main_items:
                return

            page_new = 0
            for rank, it in enumerate(main_items):
                job_id = it.get("job_id")
                job_url = it.get("job_url")
                if not job_id or not job_url:
                    continue

                if job_id in self._seen_by_search[sid]:
                    continue

                self._seen_by_search[sid].add(job_id)
                page_new += 1
                self._jobs_discovered[sid] += 1

                yield {
                    "record_type": "job_discovered",
                    "crawl_run_id": self.crawl_run_id,
                    "search_definition_id": sid,
                    "search_run_id": search_run_id,
                    "search_name": search.get("name"),
                    "source": "stepstone",
                    "job_id": job_id,
                    "job_url": job_url,
                    "rank": rank,
                    "page_start": page_num,
                    "scraped_at": fetched_at,
                    "search_url": response.url,
                }

            if page_new == 0:
                self._dup_pages[sid] += 1
            else:
                self._dup_pages[sid] = 0

            if counters and counters.main_displayed == 0:
                # We have crossed into non-main sections.
                return

            next_page = page_num + 1
            for req in self._schedule_page(search, page_num=next_page):
                yield req
        finally:
            if page:
                await page.close()

    def closed(self, reason: str):
        return
