from __future__ import annotations

import hashlib
import re
from typing import Any, Optional
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

from parsel import Selector


_JOB_PATH_RE = re.compile(r"/jobs/(?:[^/?#]*-)?(\d+)(?:[/?#]|$)")
_NON_WS_RE = re.compile(r"\s+")
_SALARY_RE = re.compile(r"[€$£]|\b\d{2,3}(?:[\.,]\d{3})+(?:\s?[–-]\s?\d{2,3}(?:[\.,]\d{3})+)?")
_BADGE_TOKENS = {
    "urgently hiring",
    "actively recruiting",
    "be an early applicant",
    "last chance",
    "new",
}


def _norm(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = _NON_WS_RE.sub(" ", value).strip()
    return v or None


def build_search_url(
    *,
    keywords: str,
    location_text: Optional[str] = None,
    city_id: Optional[str] = None,
    since_period: Optional[str] = None,
) -> str:
    kw = (keywords or "").strip()
    if not kw:
        raise ValueError("keywords cannot be empty")

    params: dict[str, str] = {"keywords": kw}
    if location_text and location_text.strip():
        params["location"] = location_text.strip()
    if city_id and city_id.strip():
        params["cityId"] = city_id.strip()
    if since_period and since_period.strip():
        params["sincePeriod"] = since_period.strip()

    return f"https://www.xing.com/jobs/search/ki?{urlencode(params)}"


def extract_job_id_from_href(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    m = _JOB_PATH_RE.search(href.strip())
    if not m:
        return None
    return m.group(1)


def canonicalize_xing_job_url(href: str, *, base_url: str = "https://www.xing.com") -> str:
    abs_url = urljoin(base_url.rstrip("/") + "/", href)
    parts = urlsplit(abs_url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def canonicalize_external_job_url(href: str, *, base_url: str = "https://www.xing.com") -> str:
    abs_url = urljoin(base_url.rstrip("/") + "/", href)
    parts = urlsplit(abs_url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def build_external_job_id(url: str) -> str:
    return f"ext_{hashlib.sha1(url.encode('utf-8')).hexdigest()[:20]}"


def _extract_highlights(card: Selector) -> list[str]:
    out: list[str] = []
    for marker in card.css("div[class*='job-teaser-facts'] span[role='status'], li[role='status']"):
        txt = marker.css("[aria-hidden='true']::text").get()
        if txt is None:
            txt = marker.xpath("normalize-space(string(.))").get()
        n = _norm(txt)
        if n:
            out.append(n)
    return out


def _classify_highlights(highlights: list[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    employment_type: Optional[str] = None
    salary_range_text: Optional[str] = None
    work_model: Optional[str] = None

    for h in highlights:
        hl = h.lower()
        if salary_range_text is None and _SALARY_RE.search(h):
            salary_range_text = h
            continue

        if work_model is None:
            if "hybrid" in hl:
                work_model = "Hybrid"
                continue
            if "remote" in hl:
                work_model = "Remote"
                continue
            if "on-site" in hl or "onsite" in hl or "vor ort" in hl:
                work_model = "On-site"
                continue

        if employment_type is None:
            if "external job ad" in hl:
                continue
            if hl in _BADGE_TOKENS:
                continue
            employment_type = h

    return employment_type, salary_range_text, work_model


def parse_search_results(html: str, *, search_url: str) -> list[dict[str, Any]]:
    sel = Selector(text=html)
    cards = sel.css('article[data-testid="job-search-result"]')

    out: list[dict[str, Any]] = []
    for idx, card in enumerate(cards):
        href = card.css("a[href]::attr(href)").get()
        if not href:
            continue

        title = _norm(card.css('[data-testid="job-teaser-list-title"]::text, [data-testid="job-teaser-card-title"]::text').get())
        company = _norm(card.css('[data-testid="job-teaser-card-company"]::text, p[class*="Company-sc"]::text').get())
        location = _norm(card.css('div[class*="multi-location-display"] p::text').get())

        posted_at = _norm(card.css("time::attr(datetime), time::attr(dateTime)").get())
        posted_time_ago = _norm(card.css("time [aria-hidden='true']::text").get())
        if not posted_time_ago:
            posted_time_ago = _norm(card.css("time::text").get())

        highlights = _extract_highlights(card)
        employment_type, salary_range_text, work_model = _classify_highlights(highlights)

        aria_label = (card.attrib.get("aria-label") or "").lower()
        has_external_marker = any("external job ad" in h.lower() for h in highlights) or ("external job ad" in aria_label)

        job_id = extract_job_id_from_href(href)
        is_external = False
        if job_id:
            job_url = canonicalize_xing_job_url(href)
        else:
            abs_url = urljoin("https://www.xing.com/", href)
            host = (urlsplit(abs_url).netloc or "").lower()
            if "xing.com" in host and not has_external_marker:
                # Skip non-job internal links.
                continue
            is_external = True
            job_url = canonicalize_external_job_url(href)
            job_id = build_external_job_id(job_url)

        if has_external_marker:
            is_external = True

        list_preview = {
            "job_title": title,
            "company_name": company,
            "job_location": location,
            "posted_at_utc": posted_at,
            "posted_time_ago": posted_time_ago,
            "employment_type": employment_type,
            "salary_range_text": salary_range_text,
            "work_model": work_model,
            "highlights": highlights,
            "external_ad": is_external,
        }

        out.append(
            {
                "source": "xing",
                "search_url": search_url,
                "job_id": job_id,
                "job_url": job_url,
                "rank": idx,
                "is_external": is_external,
                "list_preview": list_preview,
            }
        )

    return out


def has_show_more(html: str) -> bool:
    sel = Selector(text=html)
    button_texts = sel.css("button::text, button span::text, button div::text").getall()
    for txt in button_texts:
        n = " ".join((txt or "").split()).lower()
        if not n:
            continue
        if "show more" in n or "mehr anzeigen" in n:
            return True
    return False
