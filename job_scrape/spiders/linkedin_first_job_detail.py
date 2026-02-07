from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.linkedin import parse_search_results
from job_scrape.linkedin_detail import parse_job_detail


class LinkedInFirstJobDetailSpider(scrapy.Spider):
    """
    Fetch the Germany + data engineering LinkedIn guest search page, take the
    first job card, and extract structured fields from that one job detail page.
    """

    name = "linkedin_first_job_detail"
    allowed_domains = ["www.linkedin.com", "linkedin.com", "de.linkedin.com"]
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        keywords: str = "data engineering",
        location: str = "Germany",
        geo_id: str = "101282230",
        page_num: str = "0",
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

    def _build_search_url(self) -> str:
        base = "https://www.linkedin.com/jobs/search"
        params = {
            "keywords": self.keywords,
            "location": self.location,
            "geoId": self.geo_id,
            "pageNum": str(self.page_num),
            "position": "1",
            "trk": "public_jobs_jobs-search-bar_search-submit",
        }
        return f"{base}?{urlencode(params)}"

    async def start(self):
        yield scrapy.Request(self._build_search_url(), callback=self.parse_search, dont_filter=True)

    def parse_search(self, response: scrapy.http.Response):
        items = parse_search_results(response.text, search_url=response.url)
        if not items:
            self.logger.error("No job cards extracted from search page. URL=%s", response.url)
            return

        first = items[0]
        job_url = first.get("job_url")
        if not job_url:
            self.logger.error("First card missing job_url")
            return

        yield scrapy.Request(
            job_url,
            callback=self.parse_detail,
            cb_kwargs={"job_url": job_url},
            dont_filter=True,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_timeout", 1500),
                ],
            },
        )

    async def parse_detail(self, response: scrapy.http.Response, *, job_url: str):
        # Always capture the HTML we parsed for debugging selector drift.
        from pathlib import Path

        out_dir = Path("output")
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "linkedin_first_job_detail.html").write_text(response.text, encoding="utf-8")

        page = response.meta.get("playwright_page")
        if page:
            await page.screenshot(path=str(out_dir / "linkedin_first_job_detail.png"), full_page=True)
            await page.close()

        scraped_at = datetime.now(timezone.utc).isoformat()
        d = parse_job_detail(response.text)
        # Strip any extras explicitly.
        out = {
            "job_title": d.get("job_title"),
            "company_name": d.get("company_name"),
            "job_location": d.get("job_location"),
            "posted_time_ago": d.get("posted_time_ago"),
            "criteria": d.get("criteria"),
        }
        yield out
