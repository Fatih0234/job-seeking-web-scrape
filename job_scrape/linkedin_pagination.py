from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from job_scrape.linkedin import parse_search_results


SEE_MORE_BASE = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"


def build_see_more_url(
    *,
    keywords: str,
    location: str,
    geo_id: str,
    start: int,
    facets: dict[str, Any] | None = None,
) -> str:
    """
    Build the guest pagination URL used by the LinkedIn UI.
    `facets` may include f_TPR (str), f_JT/f_E/f_WT (list/str), and optional f_PP (str).
    """
    params: dict[str, Any] = {
        "keywords": keywords,
        "location": location,
        "geoId": geo_id,
        "start": str(start),
    }
    if facets:
        params.update(facets)
    return f"{SEE_MORE_BASE}?{urlencode(params, doseq=True)}"


def parse_see_more_fragment(fragment_html: str, *, search_url: str) -> list[dict[str, Any]]:
    """
    The seeMore endpoint returns a HTML fragment of <li> elements.
    Wrap in a results-list container and reuse the existing list parser.
    """
    wrapped = f"<ul class='jobs-search__results-list'>{fragment_html}</ul>"
    return parse_search_results(wrapped, search_url=search_url)

