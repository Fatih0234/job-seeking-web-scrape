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
