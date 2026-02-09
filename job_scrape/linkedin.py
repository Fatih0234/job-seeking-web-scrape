from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urljoin, urlsplit, urlunsplit

from parsel import Selector


# LinkedIn uses both:
# - /jobs/view/<id>/
# - /jobs/view/<slug>-<id>
_JOB_VIEW_RE = re.compile(r"/jobs/view/(?:[^/?#]*-)?(\d+)")
_CURRENT_JOB_ID_RE = re.compile(r"(?:[?&]currentJobId=)(\d+)")
_URN_RE = re.compile(r"urn:li:jobPosting:(\d+)")


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def extract_job_id(href: Optional[str], entity_urn: Optional[str] = None) -> Optional[str]:
    """
    Extract LinkedIn jobPosting id from:
    1) /jobs/view/<id>/
    2) currentJobId=<id>
    3) urn:li:jobPosting:<id> (entity URN)
    """
    if href:
        m = _JOB_VIEW_RE.search(href)
        if m:
            return m.group(1)
        m = _CURRENT_JOB_ID_RE.search(href)
        if m:
            return m.group(1)

    if entity_urn:
        m = _URN_RE.search(entity_urn)
        if m:
            return m.group(1)

    return None


def canonicalize_job_url(href: str, base_url: str = "https://www.linkedin.com") -> str:
    """
    Make an absolute URL and strip query/fragment to keep a stable canonical form.
    """
    absolute = urljoin(base_url, href)
    parts = urlsplit(absolute)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


@dataclass(frozen=True)
class LinkedInSearchCard:
    job_id: str
    job_url: str
    title: Optional[str]
    company: Optional[str]
    location: Optional[str]
    posted_at: Optional[str]
    rank: int


def parse_no_results_box(html: str) -> Optional[dict[str, Optional[str]]]:
    """
    Detect the LinkedIn guest jobs "no results" state.

    Example page contains:
      - section.no-results
      - .no-results__main-title-keywords
      - p.no-results__subheading

    Returns a small dict for logging/diagnostics, or None if not present.
    """
    sel = Selector(text=html)
    sec = sel.css("section.no-results")
    if not sec:
        return None

    keywords = _clean_text(sec.css(".no-results__main-title-keywords::text").get())
    # Use string(.) to include nested <strong> text etc.
    title_text = _clean_text(sec.xpath("string(.//h1)").get())
    subheading = _clean_text(sec.css("p.no-results__subheading::text").get())

    return {
        "keywords": keywords,
        "title_text": title_text,
        "subheading": subheading,
    }


def parse_search_results(html: str, *, search_url: str, base_url: str = "https://www.linkedin.com") -> list[dict[str, Any]]:
    """
    Parse LinkedIn public jobs search HTML for job cards.
    Returns serializable dicts (no scraped_at; spider adds it).
    """
    sel = Selector(text=html)
    cards = sel.css("ul.jobs-search__results-list > li")

    out: list[dict[str, Any]] = []
    for idx, card in enumerate(cards):
        href = card.css("a.base-card__full-link::attr(href)").get()
        entity_urn = card.attrib.get("data-entity-urn")

        job_id = extract_job_id(href, entity_urn=entity_urn)
        if not job_id or not href:
            # Skip cards we can't identify or link to.
            continue

        title = _clean_text(card.css("h3.base-search-card__title::text").get())
        company = _clean_text(card.css("h4.base-search-card__subtitle::text").get())
        if not company:
            company = _clean_text(card.css("a.hidden-nested-link::text").get())

        location = _clean_text(card.css("span.job-search-card__location::text").get())
        posted_at = _clean_text(card.css("time::attr(datetime)").get())
        if not posted_at:
            posted_at = _clean_text(card.css("time::text").get())

        out.append(
            {
                "source": "linkedin",
                "search_url": search_url,
                "job_id": job_id,
                "job_url": canonicalize_job_url(href, base_url=base_url),
                "title": title,
                "company": company,
                "location": location,
                "posted_at": posted_at,
                "rank": idx,
            }
        )

    return out
