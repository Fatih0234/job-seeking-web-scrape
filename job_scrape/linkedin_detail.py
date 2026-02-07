from __future__ import annotations

import re
from typing import Any, Optional

from parsel import Selector


_WS_RE = re.compile(r"\s+")


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = _WS_RE.sub(" ", s).strip()
    return s2 or None


def _text(sel) -> Optional[str]:
    if sel is None:
        return None
    if not sel:
        return None
    # string(.) pulls all descendant text, which is more robust than ::text for these nodes.
    try:
        # SelectorList -> first element (parsel.Selector has `.root`)
        if hasattr(sel, "__len__") and not hasattr(sel, "root"):
            sel = sel[0] if sel else None
            if sel is None:
                return None
        return _norm(sel.xpath("string(.)").get())
    except Exception:
        return None


def parse_job_detail(html: str) -> dict[str, Any]:
    """
    Extracts only:
    - job_title
    - company_name
    - job_location
    - posted_time_ago
    - job_description
    - criteria: {seniority_level, employment_type, job_function, industries}
    """
    sel = Selector(text=html)

    # Top section fields
    job_title = _text(sel.css("h2.top-card-layout__title"))
    if not job_title:
        job_title = _text(sel.css("h2.topcard__title"))
    if not job_title:
        job_title = _text(sel.css("h1.top-card-layout__title"))
    if not job_title:
        job_title = _text(sel.css("h1.topcard__title"))

    # company_name
    company_name = _text(sel.css("a.topcard__org-name-link"))
    if not company_name:
        company_name = _text(sel.css(".topcard__flavor a[href*='/company/']"))

    # job_location
    job_location = _text(sel.css(".topcard__flavor-row .topcard__flavor--bullet"))

    posted_time_ago = _text(sel.css("span.posted-time-ago__text"))

    # Description
    job_description = _text(sel.css("div.description__text--rich div.show-more-less-html__markup"))
    if not job_description:
        job_description = _text(sel.css("div.description__text--rich"))

    criteria = {
        "seniority_level": None,
        "employment_type": None,
        "job_function": None,
        "industries": None,
    }

    label_map = {
        "seniority level": "seniority_level",
        "employment type": "employment_type",
        "job function": "job_function",
        "industries": "industries",
        # Common non-English labels seen on regional domains (e.g. de.linkedin.com)
        "karrierestufe": "seniority_level",
        "beschäftigungsverhältnis": "employment_type",
        "taetigkeitsbereich": "job_function",
        "tätigkeitsbereich": "job_function",
        "branchen": "industries",
    }

    for li in sel.css("ul.description__job-criteria-list li.description__job-criteria-item"):
        label = _text(li.css("h3.description__job-criteria-subheader"))
        value = _text(li.css("span.description__job-criteria-text--criteria"))
        if not label:
            continue
        key = label_map.get(label.strip().lower())
        if key:
            criteria[key] = value

    return {
        "job_title": job_title,
        "company_name": company_name,
        "job_location": job_location,
        "posted_time_ago": posted_time_ago,
        "job_description": job_description,
        "criteria": criteria,
    }
