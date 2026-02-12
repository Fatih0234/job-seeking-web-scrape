from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

from parsel import Selector


_JOB_ITEM_RE = re.compile(r"^job-item-(\d+)$")

_MARKER_PATTERNS = (
    re.compile(r"noch\s+nichts\s+dabei", re.IGNORECASE),
    re.compile(r"au[ßs]erhalb\s+deiner\s+region", re.IGNORECASE),
)


@dataclass(frozen=True)
class StepstoneResultCounters:
    total: Optional[int] = None
    main: Optional[int] = None
    regional: Optional[int] = None
    semantic: Optional[int] = None
    recommended: Optional[int] = None
    main_displayed: Optional[int] = None
    regional_displayed: Optional[int] = None
    semantic_displayed: Optional[int] = None
    recommended_displayed: Optional[int] = None


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def _slugify(value: str) -> str:
    s = unicodedata.normalize("NFKC", value or "").strip().lower()
    if not s:
        return ""
    s = s.replace("/", "-")
    s = re.sub(r"\s+", "-", s)
    # Keep letters (including umlauts), digits, and dashes.
    s = re.sub(r"[^0-9a-z\u00c0-\u024f-]", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def normalize_sort(value: Any) -> int:
    if isinstance(value, int):
        if value in {1, 2}:
            return value
        raise ValueError(f"Unsupported sort value: {value}")

    s = str(value).strip().lower()
    if s in {"1", "relevance", "relevant", "sort_relevance"}:
        return 1
    if s in {"2", "newest", "date", "publish", "published", "sort_publish"}:
        return 2
    raise ValueError(f"Unsupported sort value: {value}")


def sort_action(sort_value: int, *, page: int = 1) -> Optional[str]:
    if page > 1:
        return "paging_next"
    if sort_value == 1:
        return "sort_relevance"
    if sort_value == 2:
        return "sort_publish"
    return None


def normalize_age_days(value: Any) -> int:
    if isinstance(value, int):
        if value in {1, 7}:
            return value
        raise ValueError(f"Unsupported age_days value: {value}")

    s = str(value).strip().lower()
    if s in {"1", "age_1"}:
        return 1
    if s in {"7", "age_7"}:
        return 7
    raise ValueError(f"Unsupported age_days value: {value}")


def build_search_url(
    *,
    keywords: str,
    location: str,
    radius: int = 30,
    sort: int | str = 2,
    age_days: int | str | None = None,
    page: int = 1,
    action: Optional[str] = None,
    search_origin: str = "Resultlist_top-search",
    where_type: str = "autosuggest",
) -> str:
    kw_slug = _slugify(keywords)
    loc_slug = _slugify(location)

    if not kw_slug:
        raise ValueError("keywords cannot be empty")
    if not loc_slug:
        raise ValueError("location cannot be empty")

    sort_num = normalize_sort(sort)

    path = f"/jobs/{kw_slug}/in-{loc_slug}"

    if radius <= 0:
        raise ValueError("radius must be > 0")

    params: dict[str, Any] = {
        "radius": str(radius),
        "sort": str(sort_num),
        "searchOrigin": search_origin,
    }
    if where_type:
        params["whereType"] = where_type

    if page > 1:
        params["page"] = str(page)

    age_num: Optional[int] = None
    if age_days is not None:
        age_num = normalize_age_days(age_days)
        params["ag"] = f"age_{age_num}"

    action_value = action
    if action_value is None:
        if age_num is not None and page == 1:
            action_value = f"facet_selected;age;age_{age_num}"
        elif page > 1:
            action_value = "paging_next"
        else:
            action_value = sort_action(sort_num, page=page)

    if action_value:
        params["action"] = action_value

    return f"https://www.stepstone.de{path}?{urlencode(params, doseq=True)}"


def canonicalize_stepstone_job_url(job_id: str, base_url: str = "https://www.stepstone.de") -> str:
    base = base_url.rstrip("/")
    return f"{base}/job/{job_id}"


def extract_job_id(article_id: Optional[str]) -> Optional[str]:
    if not article_id:
        return None
    m = _JOB_ITEM_RE.match(article_id.strip())
    if not m:
        return None
    return m.group(1)


def parse_result_counters(html: str) -> Optional[StepstoneResultCounters]:
    sel = Selector(text=html)
    node = sel.css("[data-resultlist-offers-total]")
    if not node:
        return None

    attrs = node.attrib

    def _i(name: str) -> Optional[int]:
        v = attrs.get(name)
        if v is None or v == "":
            return None
        try:
            return int(v)
        except ValueError:
            return None

    return StepstoneResultCounters(
        total=_i("data-resultlist-offers-total"),
        main=_i("data-resultlist-offers-main"),
        regional=_i("data-resultlist-offers-regional"),
        semantic=_i("data-resultlist-offers-semantic"),
        recommended=_i("data-resultlist-offers-recommended"),
        main_displayed=_i("data-resultlist-offers-main-displayed"),
        regional_displayed=_i("data-resultlist-offers-regional-displayed"),
        semantic_displayed=_i("data-resultlist-offers-semantic-displayed"),
        recommended_displayed=_i("data-resultlist-offers-recommended-displayed"),
    )


def parse_section_markers(html: str) -> list[str]:
    sel = Selector(text=html)
    out: list[str] = []
    for txt in sel.css("h3::text, h4::text").getall():
        clean = _clean_text(txt)
        if not clean:
            continue
        if any(p.search(clean) for p in _MARKER_PATTERNS):
            out.append(clean)
    return out


def parse_search_results(html: str, *, search_url: str) -> list[dict[str, Any]]:
    sel = Selector(text=html)
    cards = sel.css('article[id^="job-item-"]')

    out: list[dict[str, Any]] = []
    for idx, card in enumerate(cards):
        article_id = card.attrib.get("id")
        job_id = extract_job_id(article_id)
        if not job_id:
            continue

        out.append(
            {
                "source": "stepstone",
                "search_url": search_url,
                "job_id": job_id,
                "job_url": canonicalize_stepstone_job_url(job_id),
                "rank": idx,
            }
        )

    return out


def select_main_results(
    results: list[dict[str, Any]],
    *,
    counters: Optional[StepstoneResultCounters],
) -> list[dict[str, Any]]:
    if counters and counters.main_displayed is not None:
        n = max(0, counters.main_displayed)
        return results[:n]
    return results


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
