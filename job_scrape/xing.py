from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

from parsel import Selector


_JOB_PATH_RE = re.compile(r"/jobs/(?:[^/?#]*-)?(\d+)(?:[/?#]|$)")


def build_search_url(*, keywords: str, location_text: Optional[str] = None, city_id: Optional[str] = None) -> str:
    kw = (keywords or "").strip()
    if not kw:
        raise ValueError("keywords cannot be empty")

    params: dict[str, str] = {"keywords": kw}
    if location_text and location_text.strip():
        params["location"] = location_text.strip()
    if city_id and city_id.strip():
        params["cityId"] = city_id.strip()

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


def parse_search_results(html: str, *, search_url: str) -> list[dict[str, Any]]:
    sel = Selector(text=html)
    cards = sel.css('article[data-testid="job-search-result"]')

    out: list[dict[str, Any]] = []
    for idx, card in enumerate(cards):
        hrefs = card.css('a[href*="/jobs/"]::attr(href)').getall()
        href = next((h for h in hrefs if extract_job_id_from_href(h)), None)
        if not href:
            continue

        job_id = extract_job_id_from_href(href)
        if not job_id:
            continue

        out.append(
            {
                "source": "xing",
                "search_url": search_url,
                "job_id": job_id,
                "job_url": canonicalize_xing_job_url(href),
                "rank": idx,
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

