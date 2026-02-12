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


def _string_value(sel: Selector, css: str) -> Optional[str]:
    node = sel.css(css)
    if not node:
        return None
    return _desc_text(node[0])


def _desc_text(node: Selector) -> Optional[str]:
    # Drop style/script nodes before extracting text.
    clone = Selector(text=node.get())
    clone.css("style, script").drop()
    txt = clone.xpath("normalize-space(string(.))").get()
    return _norm(txt)


def parse_job_detail(html: str) -> dict[str, Any]:
    sel = Selector(text=html)

    job_title = _string_value(sel, "h1")

    company_name = _string_value(sel, '[data-at="metadata-company-name"]')
    job_location = _string_value(sel, '[data-at="metadata-location"]')

    posted_raw = _string_value(sel, '[data-at="metadata-online-date"]')
    posted_time_ago = posted_raw
    if posted_time_ago:
        posted_time_ago = re.sub(r"^Erschienen:\s*", "", posted_time_ago, flags=re.IGNORECASE)
        posted_time_ago = _norm(posted_time_ago)

    contract_type = _string_value(sel, '[data-at="metadata-contract-type"]')
    work_type = _string_value(sel, '[data-at="metadata-work-type"]')

    desc_sections: list[str] = []
    for span in sel.css('[data-at="job-ad-content"] span.job-ad-display-nfizss'):
        txt = _desc_text(span)
        if txt:
            desc_sections.append(txt)

    # Salary section often uses the same class name; explicitly exclude it.
    salary_texts = {
        txt
        for txt in (
            _desc_text(span) for span in sel.css('[data-at="job-ad-salary"] span.job-ad-display-nfizss')
        )
        if txt
    }
    if salary_texts:
        desc_sections = [x for x in desc_sections if x not in salary_texts]

    job_description = _norm("\n\n".join(desc_sections))

    criteria = {
        "contract_type": contract_type,
        "work_type": work_type,
    }

    return {
        "job_title": job_title,
        "company_name": company_name,
        "job_location": job_location,
        "posted_time_ago": posted_time_ago,
        "job_description": job_description,
        "criteria": criteria,
    }
