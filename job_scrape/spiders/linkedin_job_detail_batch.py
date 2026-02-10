from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scrapy
from scrapy_playwright.page import PageMethod

from job_scrape.linkedin_detail import parse_job_detail


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


class LinkedInJobDetailBatchSpider(scrapy.Spider):
    """
    Fetch a batch of LinkedIn job detail pages and extract structured fields.

    Inputs:
    - inputs: JSON file with {"jobs": [{"source","job_id","job_url"}...]}
    - crawl_run_id: optional
    """

    name = "linkedin_job_detail_batch"
    allowed_domains = ["www.linkedin.com", "linkedin.com", "de.linkedin.com"]
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        # Details are the most block-prone part; keep this spider extra conservative.
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 4.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
    }

    def __init__(self, inputs: str, crawl_run_id: str = "", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.inputs_path = inputs
        self.crawl_run_id = crawl_run_id or None
        self._allow_playwright_fallback = (
            str(os.getenv("DETAIL_ALLOW_PLAYWRIGHT_FALLBACK", "")).strip().lower()
            in {"1", "true", "yes", "y", "on"}
        )

        # Debug artifacts for failures (limited)
        self._failure_debug_limit = 5
        try:
            from job_scrape.runtime import budgets

            self._failure_debug_limit = budgets()["DETAIL_DEBUG_FAILURE_LIMIT"]
        except Exception:
            self._failure_debug_limit = 5
        self._failure_debug_count = 0
        self._block_streak = 0
        try:
            from job_scrape.runtime import budgets

            self._block_streak_limit = budgets()["CIRCUIT_BREAKER_BLOCKS"]
        except Exception:
            self._block_streak_limit = 3

    @staticmethod
    def _guest_posting_url(job_id: str) -> str:
        return f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"

    async def start(self):
        data = json.loads(Path(self.inputs_path).read_text(encoding="utf-8"))
        jobs = data.get("jobs") or []
        if not jobs:
            self.logger.error("No jobs in inputs file: %s", self.inputs_path)
            return

        for j in jobs:
            job_id = str(j.get("job_id") or "").strip()
            if not job_id:
                continue
            url = self._guest_posting_url(job_id)
            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                cb_kwargs={"job": j, "used_playwright": False},
                dont_filter=True,
            )

    async def parse_detail(self, response: scrapy.http.Response, *, job: dict[str, Any], used_playwright: bool):
        fetched_at = datetime.now(timezone.utc).isoformat()

        blocked = _looks_blocked(response)
        if blocked:
            self._block_streak += 1
            page = response.meta.get("playwright_page")
            if page:
                await page.close()
            yield {
                "record_type": "job_detail",
                "crawl_run_id": self.crawl_run_id,
                "source": job.get("source", "linkedin"),
                "job_id": job.get("job_id"),
                "job_url": job.get("job_url"),
                "scraped_at": fetched_at,
                "parse_ok": False,
                "blocked": True,
                "used_playwright": used_playwright,
                "last_error": "blocked",
                "job_title": None,
                "company_name": None,
                "job_location": None,
                "posted_time_ago": None,
                "job_description": None,
                "criteria": {},
            }
            if self._block_streak >= self._block_streak_limit:
                # Stop early if we're consistently blocked (circuit breaker).
                try:
                    self.crawler.engine.close_spider(self, reason="blocked_circuit_breaker")
                except Exception:
                    pass
            return

        self._block_streak = 0
        parsed = parse_job_detail(response.text)
        critical_missing = parsed.get("job_title") is None

        if critical_missing and (not used_playwright) and self._allow_playwright_fallback:
            # Retry once with Playwright rendering.
            yield scrapy.Request(
                job["job_url"],
                callback=self.parse_detail,
                cb_kwargs={"job": job, "used_playwright": True},
                dont_filter=True,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        # Wait a bit for the detail page to render.
                        PageMethod("wait_for_timeout", 1500),
                    ],
                },
            )
            return

        page = response.meta.get("playwright_page")
        if critical_missing and self._failure_debug_count < self._failure_debug_limit:
            out_dir = Path("output") / "detail_failures"
            out_dir.mkdir(parents=True, exist_ok=True)
            suffix = f"{job.get('job_id','unknown')}_{'pw' if used_playwright else 'http'}"
            (out_dir / f"{suffix}.html").write_text(response.text, encoding="utf-8")
            if page:
                await page.screenshot(path=str(out_dir / f"{suffix}.png"), full_page=True)
            self._failure_debug_count += 1

        if page:
            await page.close()

        yield {
            "record_type": "job_detail",
            "crawl_run_id": self.crawl_run_id,
            "source": job.get("source", "linkedin"),
            "job_id": job.get("job_id"),
            "job_url": job.get("job_url"),
            "scraped_at": fetched_at,
            "parse_ok": not critical_missing,
            "blocked": False,
            "used_playwright": used_playwright,
            "last_error": None if not critical_missing else "missing_job_title",
            "job_title": parsed.get("job_title"),
            "company_name": parsed.get("company_name"),
            "job_location": parsed.get("job_location"),
            "posted_time_ago": parsed.get("posted_time_ago"),
            "job_description": parsed.get("job_description"),
            "criteria": parsed.get("criteria") or {},
        }
