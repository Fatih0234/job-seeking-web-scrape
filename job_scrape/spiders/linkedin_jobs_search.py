from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.linkedin import parse_search_results


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


class LinkedInJobsSearchSpider(scrapy.Spider):
    name = "linkedin_jobs_search"
    allowed_domains = ["www.linkedin.com", "linkedin.com"]

    # LinkedIn's robots.txt likely disallows this path; keep global defaults
    # conservative, but allow this milestone spider to run.
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "viewport": {"width": 1280, "height": 720},
            }
        },
    }

    def __init__(
        self,
        keywords: str = "data engineering",
        location: str = "Germany",
        geo_id: str = "101282230",
        page_num: str = "0",
        include_tracking_params: str = "true",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.keywords = keywords
        self.location = location
        self.geo_id = geo_id
        try:
            self.page_num = int(page_num)
        except ValueError:
            self.page_num = 0
        self.include_tracking_params = _parse_bool(include_tracking_params, default=True)

    def _build_search_url(self) -> str:
        base = "https://www.linkedin.com/jobs/search"

        params: dict[str, Any] = {
            "keywords": self.keywords,
            "location": self.location,
            "geoId": self.geo_id,
            "pageNum": str(self.page_num),
        }

        if self.include_tracking_params:
            # These are copied from the example URL for reproducibility. They
            # should not be required to fetch the list page.
            params.update(
                {
                    "trk": "public_jobs_jobs-search-bar_search-submit",
                    "currentJobId": "4064488192",
                    "position": "1",
                }
            )

        return f"{base}?{urlencode(params)}"

    async def start(self):
        url = self._build_search_url()

        yield scrapy.Request(
            url,
            callback=self.parse,
            errback=self.errback,
            dont_filter=True,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "ul.jobs-search__results-list"),
                    PageMethod("wait_for_timeout", 1000),
                    PageMethod("evaluate", "window.scrollBy(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1000),
                    PageMethod("evaluate", "window.scrollBy(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1000),
                ],
            },
        )

    async def parse(self, response: scrapy.http.Response):
        page = response.meta.get("playwright_page")
        try:
            scraped_at = datetime.now(timezone.utc).isoformat()
            items = parse_search_results(response.text, search_url=response.url)
            if not items:
                # If we extracted 0 items, this can be either a selector drift or a block page.
                body_l = response.text.lower()
                looks_blocked = (
                    "/checkpoint/" in response.url
                    or "security verification" in body_l
                    or "verify you are a human" in body_l
                    or "unusual activity" in body_l
                )
                if looks_blocked:
                    from pathlib import Path

                    out_dir = Path("output")
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "linkedin_blocked.html").write_text(response.text, encoding="utf-8")
                    if page:
                        await page.screenshot(path=str(out_dir / "linkedin_blocked.png"), full_page=True)
                    self.logger.error(
                        "LinkedIn appears to have blocked the request. "
                        "Saved output/linkedin_blocked.html and output/linkedin_blocked.png for inspection."
                    )
                else:
                    self.logger.warning("No job cards extracted. Selectors may have changed or results are empty.")

            for it in items:
                it["scraped_at"] = scraped_at
                yield it
        finally:
            if page:
                await page.close()

    async def errback(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error("Request failed: %r", failure)
