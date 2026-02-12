from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.runtime import budgets
from job_scrape.stepstone_detail import parse_job_detail


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


class StepstoneJobDetailBatchSpider(scrapy.Spider):
    """
    Fetch a batch of Stepstone job detail pages (Playwright-first) and extract fields.

    Inputs:
    - inputs: JSON file with {"jobs": [{"source","job_id","job_url"} ...]}
    - crawl_run_id: optional
    """

    name = "stepstone_job_detail_batch"
    allowed_domains = ["www.stepstone.de", "stepstone.de"]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 3.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
    }

    def __init__(self, inputs: str, crawl_run_id: str = "", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.inputs_path = inputs
        self.crawl_run_id = crawl_run_id or None

        b = budgets()
        self._failure_debug_limit = b["DETAIL_DEBUG_FAILURE_LIMIT"]
        self._failure_debug_count = 0
        self._block_streak = 0
        self._block_streak_limit = b["CIRCUIT_BREAKER_BLOCKS"]

    async def start(self):
        data = json.loads(Path(self.inputs_path).read_text(encoding="utf-8"))
        jobs = data.get("jobs") or []
        if not jobs:
            self.logger.error("No jobs in inputs file: %s", self.inputs_path)
            return

        for j in jobs:
            job_url = str(j.get("job_url") or "").strip()
            if not job_url:
                continue

            yield scrapy.Request(
                job_url,
                callback=self.parse_detail,
                cb_kwargs={"job": j},
                dont_filter=True,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 1200),
                    ],
                },
            )

    async def parse_detail(self, response: scrapy.http.Response, *, job: dict[str, Any]):
        fetched_at = datetime.now(timezone.utc).isoformat()
        page = response.meta.get("playwright_page")

        try:
            blocked = _looks_blocked(response)
            if blocked:
                self._block_streak += 1
                yield {
                    "record_type": "job_detail",
                    "crawl_run_id": self.crawl_run_id,
                    "source": "stepstone",
                    "job_id": job.get("job_id"),
                    "job_url": job.get("job_url"),
                    "scraped_at": fetched_at,
                    "parse_ok": False,
                    "blocked": True,
                    "used_playwright": True,
                    "last_error": "blocked",
                    "job_title": None,
                    "company_name": None,
                    "job_location": None,
                    "posted_time_ago": None,
                    "job_description": None,
                    "criteria": {},
                }
                if self._block_streak >= self._block_streak_limit:
                    try:
                        self.crawler.engine.close_spider(self, reason="blocked_circuit_breaker")
                    except Exception:
                        pass
                return

            self._block_streak = 0

            parsed = parse_job_detail(response.text)
            critical_missing = parsed.get("job_title") is None

            if critical_missing and self._failure_debug_count < self._failure_debug_limit:
                out_dir = Path("output") / "stepstone_detail_failures"
                out_dir.mkdir(parents=True, exist_ok=True)
                suffix = f"{job.get('job_id','unknown')}_pw"
                (out_dir / f"{suffix}.html").write_text(response.text, encoding="utf-8")
                if page:
                    await page.screenshot(path=str(out_dir / f"{suffix}.png"), full_page=True)
                self._failure_debug_count += 1

            yield {
                "record_type": "job_detail",
                "crawl_run_id": self.crawl_run_id,
                "source": "stepstone",
                "job_id": job.get("job_id"),
                "job_url": job.get("job_url"),
                "scraped_at": fetched_at,
                "parse_ok": not critical_missing,
                "blocked": False,
                "used_playwright": True,
                "last_error": None if not critical_missing else "missing_job_title",
                "job_title": parsed.get("job_title"),
                "company_name": parsed.get("company_name"),
                "job_location": parsed.get("job_location"),
                "posted_time_ago": parsed.get("posted_time_ago"),
                "job_description": parsed.get("job_description"),
                "criteria": parsed.get("criteria") or {},
            }
        finally:
            if page:
                await page.close()
