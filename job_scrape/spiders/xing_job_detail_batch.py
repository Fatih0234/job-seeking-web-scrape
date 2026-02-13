from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.runtime import budgets
from job_scrape.xing_detail import parse_job_detail


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


class XingJobDetailBatchSpider(scrapy.Spider):
    """
    Fetch a batch of XING job detail pages (Playwright-first) and extract fields.

    Inputs:
    - inputs: JSON file with {"jobs": [{"job_id","job_url"} ...]}
    - crawl_run_id: optional
    """

    name = "xing_job_detail_batch"
    allowed_domains = ["www.xing.com", "xing.com", "click.appcast.io"]
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
                errback=self.parse_detail_error,
                cb_kwargs={"job": j},
                dont_filter=True,
                headers={"Accept-Language": "en-US,en;q=0.9"},
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 45_000,
                    },
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 1200),
                    ],
                    "handle_httpstatus_all": True,
                    "download_timeout": 90,
                    "job_ctx": j,
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
                    "source": "xing",
                    "job_id": job.get("job_id"),
                    "job_url": job.get("job_url"),
                    "scraped_at": fetched_at,
                    "parse_ok": False,
                    "blocked": True,
                    "used_playwright": True,
                    "last_error": "blocked",
                    "posted_at_utc": None,
                    "posted_time_ago": None,
                    "job_title": None,
                    "company_name": None,
                    "job_location": None,
                    "employment_type": None,
                    "salary_range_text": None,
                    "work_model": None,
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

            html = response.text or ""
            parsed = parse_job_detail(html)
            http_status_error = response.status >= 400
            critical_missing = parsed.get("job_title") is None

            # If we got a sparse HTML snapshot, try a second pass using the live Playwright DOM.
            if critical_missing and page:
                try:
                    await page.wait_for_timeout(2500)
                    html2 = await page.content()
                    parsed2 = parse_job_detail(html2 or "")
                    if parsed2.get("job_title") is not None:
                        parsed = parsed2
                        critical_missing = False
                except Exception:
                    pass

            if critical_missing and self._failure_debug_count < self._failure_debug_limit:
                out_dir = Path("output") / "xing_detail_failures"
                out_dir.mkdir(parents=True, exist_ok=True)
                suffix = f"{job.get('job_id','unknown')}_pw"
                (out_dir / f"{suffix}.html").write_text(html, encoding="utf-8")
                if page:
                    await page.screenshot(path=str(out_dir / f"{suffix}.png"), full_page=True)
                self._failure_debug_count += 1

            yield {
                "record_type": "job_detail",
                "crawl_run_id": self.crawl_run_id,
                "source": "xing",
                "job_id": job.get("job_id"),
                "job_url": job.get("job_url"),
                "scraped_at": fetched_at,
                "parse_ok": (not critical_missing) and (not http_status_error),
                "blocked": False,
                "used_playwright": True,
                "last_error": (
                    f"http_{response.status}"
                    if http_status_error
                    else (None if not critical_missing else "missing_job_title")
                ),
                "posted_at_utc": parsed.get("posted_at_utc"),
                "posted_time_ago": parsed.get("posted_time_ago"),
                "job_title": parsed.get("job_title"),
                "company_name": parsed.get("company_name"),
                "job_location": parsed.get("job_location"),
                "employment_type": parsed.get("employment_type"),
                "salary_range_text": parsed.get("salary_range_text"),
                "work_model": parsed.get("work_model"),
                "job_description": parsed.get("job_description"),
                "criteria": {
                    **(parsed.get("criteria") or {}),
                    "http_status": response.status,
                },
            }
        finally:
            if page:
                await page.close()

    async def parse_detail_error(self, failure):
        request = failure.request
        job = request.meta.get("job_ctx") or {}
        page = request.meta.get("playwright_page")
        fetched_at = datetime.now(timezone.utc).isoformat()
        try:
            yield {
                "record_type": "job_detail",
                "crawl_run_id": self.crawl_run_id,
                "source": "xing",
                "job_id": job.get("job_id"),
                "job_url": job.get("job_url"),
                "scraped_at": fetched_at,
                "parse_ok": False,
                "blocked": False,
                "used_playwright": True,
                "last_error": failure.getErrorMessage()[:500],
                "posted_at_utc": None,
                "posted_time_ago": None,
                "job_title": None,
                "company_name": None,
                "job_location": None,
                "employment_type": None,
                "salary_range_text": None,
                "work_model": None,
                "job_description": None,
                "criteria": {
                    "http_status": getattr(getattr(failure.value, "response", None), "status", None),
                    "request_failure": True,
                },
            }
        finally:
            if page:
                await page.close()
