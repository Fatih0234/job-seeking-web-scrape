from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.runtime import budgets
from job_scrape.xing import build_search_url, has_show_more, parse_search_results


def _looks_blocked(status: int, html: str) -> bool:
    if status in {403, 429, 503}:
        return True
    body_l = html.lower()
    return (
        "access denied" in body_l
        or "verify you are a human" in body_l
        or "captcha" in body_l
        or "temporarily blocked" in body_l
        or "errors.edgesuite.net" in body_l
    )


class XingDiscoveryPaginatedSpider(scrapy.Spider):
    """
    Discover XING jobs by repeatedly clicking "Show more".

    Inputs:
    - inputs: path to JSON with {"searches": [{...}]}
      Each search should include:
        - search_definition_id
        - name
        - keywords
        - location_text (optional)
        - facets (optional, supports city_id)

    Output:
    - record_type=job_discovered items
    - record_type=page_fetch items
    """

    name = "xing_discovery_paginated"
    allowed_domains = ["www.xing.com", "xing.com"]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2.0,
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

            for req in self._schedule_search(s):
                yield req

    def _schedule_search(self, s: dict[str, Any]):
        sid = str(s["search_definition_id"])
        if self._blocked.get(sid):
            return

        facets = s.get("facets") or {}
        city_id = facets.get("city_id")
        # Allow incremental runs (GitHub Actions) without modifying DB definitions.
        since_period = (os.getenv("XING_SINCE_PERIOD") or "").strip() or facets.get("since_period")
        url = build_search_url(
            keywords=s.get("keywords", ""),
            location_text=s.get("location_text"),
            city_id=city_id,
            since_period=since_period,
        )

        yield scrapy.Request(
            url,
            callback=self.parse_search,
            cb_kwargs={"search": s},
            dont_filter=True,
            headers={"Accept-Language": "en-US,en;q=0.9"},
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_timeout", 1200),
                ],
            },
        )

    @staticmethod
    async def _click_show_more(page) -> bool:
        selectors = (
            'button:has-text("Show more")',
            'button:has-text("Mehr anzeigen")',
        )
        for sel in selectors:
            btn = page.locator(sel).first
            if await btn.count() == 0:
                continue
            if not await btn.is_visible():
                continue
            if not await btn.is_enabled():
                continue
            try:
                await btn.click(timeout=5000)
                return True
            except Exception:
                continue
        return False

    async def parse_search(self, response: scrapy.http.Response, *, search: dict[str, Any]):
        sid = str(search["search_definition_id"])
        search_run_id = search.get("search_run_id")
        page = response.meta.get("playwright_page")

        try:
            page_batch = 0
            current_html = response.text
            current_url = response.url

            while True:
                if self._pages_fetched[sid] >= self._b["MAX_PAGES_PER_SEARCH"]:
                    return
                if self._jobs_discovered[sid] >= self._b["MAX_JOBS_DISCOVERED_PER_SEARCH"]:
                    return
                if self._dup_pages[sid] >= self._b["DUPLICATE_PAGE_LIMIT"]:
                    return

                self._pages_fetched[sid] += 1
                fetched_at = datetime.now(timezone.utc).isoformat()

                blocked = _looks_blocked(response.status, current_html)
                if blocked:
                    self._block_streak[sid] += 1
                    yield {
                        "record_type": "page_fetch",
                        "crawl_run_id": self.crawl_run_id,
                        "search_definition_id": sid,
                        "search_run_id": search_run_id,
                        "search_name": search.get("name"),
                        "page_start": page_batch,
                        "status_code": response.status,
                        "blocked": True,
                        "item_count": 0,
                        "fetched_at": fetched_at,
                        "url": current_url,
                    }
                    if self._block_streak[sid] >= self._b["CIRCUIT_BREAKER_BLOCKS"]:
                        self._blocked[sid] = True
                        self.logger.error("Blocked detected for search %s; stopping (circuit breaker).", sid)
                    return

                self._block_streak[sid] = 0
                items = parse_search_results(current_html, search_url=current_url)
                yield {
                    "record_type": "page_fetch",
                    "crawl_run_id": self.crawl_run_id,
                    "search_definition_id": sid,
                    "search_run_id": search_run_id,
                    "search_name": search.get("name"),
                    "page_start": page_batch,
                    "status_code": response.status,
                    "blocked": False,
                    "item_count": len(items),
                    "fetched_at": fetched_at,
                    "url": current_url,
                }

                if not items:
                    return

                page_new = 0
                for rank, it in enumerate(items):
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
                        "source": "xing",
                        "job_id": job_id,
                        "job_url": job_url,
                        "rank": rank,
                        "page_start": page_batch,
                        "scraped_at": fetched_at,
                        "search_url": current_url,
                        "is_external": bool(it.get("is_external")),
                        "list_preview": it.get("list_preview") or {},
                    }

                if page_new == 0:
                    self._dup_pages[sid] += 1
                else:
                    self._dup_pages[sid] = 0

                if self._pages_fetched[sid] >= self._b["MAX_PAGES_PER_SEARCH"]:
                    return
                if self._jobs_discovered[sid] >= self._b["MAX_JOBS_DISCOVERED_PER_SEARCH"]:
                    return
                if self._dup_pages[sid] >= self._b["DUPLICATE_PAGE_LIMIT"]:
                    return
                if not has_show_more(current_html):
                    return
                if not page:
                    return

                clicked = await self._click_show_more(page)
                if not clicked:
                    return

                await page.wait_for_timeout(1200)
                current_html = await page.content()
                current_url = page.url
                page_batch += 1
        finally:
            if page:
                await page.close()

    def closed(self, reason: str):
        return
