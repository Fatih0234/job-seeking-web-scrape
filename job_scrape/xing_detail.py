from __future__ import annotations

import json
import re
from datetime import datetime
from html import unescape
from typing import Any, Optional

from parsel import Selector


_WS_RE = re.compile(r"\s+")
_SALARY_RE = re.compile(r"[€$£]|\b\d{2,3}(?:[\.,]\d{3})+(?:\s?[–-]\s?\d{2,3}(?:[\.,]\d{3})+)?")
_UNDEFINED_RE = re.compile(r"\bundefined\b")
_BADGE_TOKENS = {
    "urgently hiring",
    "actively recruiting",
    "be an early applicant",
    "last chance",
    "new",
}


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = _WS_RE.sub(" ", s).strip()
    return s2 or None


def _text(node) -> Optional[str]:
    if node is None:
        return None
    if not node:
        return None
    try:
        if hasattr(node, "__len__") and not hasattr(node, "root"):
            node = node[0] if node else None
            if node is None:
                return None
        return _norm(node.xpath("string(.)").get())
    except Exception:
        return None


def _html_fragment_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    frag = Selector(text=f"<div>{html}</div>")
    chunks: list[str] = []
    for n in frag.css("h1, h2, h3, h4, h5, h6, p, li"):
        t = _norm(n.xpath("string(.)").get())
        if t:
            chunks.append(t)
    if chunks:
        return _norm("\n\n".join(chunks))
    return _norm(frag.xpath("normalize-space(string(.))").get())


def _parse_jobposting_json_ld(sel: Selector) -> dict[str, Any]:
    for raw in sel.css('script[type="application/ld+json"]::text').getall():
        raw = (raw or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                return obj
    return {}


def _parse_runtime_config_apollo(sel: Selector) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    XING job detail pages sometimes omit JSON-LD and SSR DOM content in the HTML snapshot,
    but include a JS runtime-config with Apollo state. We can extract core fields from that.

    Returns: (visible_job_obj, meta) where meta includes the Apollo store for deref.
    """
    raw = (sel.css("script#runtime-config::text").get() or "").strip()
    if not raw.startswith("window.crate="):
        return {}, {}

    js = raw[len("window.crate=") :]
    if js.endswith(";"):
        js = js[:-1]

    # JS -> JSON normalization (common non-JSON literal).
    js = _UNDEFINED_RE.sub("null", js)
    try:
        data = json.loads(js)
    except Exception:
        return {}, {}

    apollo = (data.get("serverData") or {}).get("APOLLO_STATE") or {}
    if not isinstance(apollo, dict):
        return {}, {}

    root = apollo.get("ROOT_QUERY") if isinstance(apollo.get("ROOT_QUERY"), dict) else {}
    ref = None
    if isinstance(root, dict):
        for v in root.values():
            if isinstance(v, dict):
                r = v.get("__ref")
                if isinstance(r, str) and r.startswith("VisibleJob:"):
                    ref = r
                    break

    if not ref:
        for k in apollo.keys():
            if isinstance(k, str) and k.startswith("VisibleJob:"):
                ref = k
                break

    job = apollo.get(ref) if ref else None
    if not isinstance(job, dict):
        return {}, {}

    meta = {
        "source": "apollo_runtime_config",
        "ref": ref,
        "_apollo": apollo,
    }
    return job, meta


def _apollo_deref(meta: dict[str, Any], v: Any) -> Any:
    apollo = meta.get("_apollo") or {}
    if isinstance(v, dict) and "__ref" in v and isinstance(apollo, dict):
        return apollo.get(v["__ref"])
    return v


def _stringify_apollo_location(loc: Any) -> Optional[str]:
    if not isinstance(loc, dict):
        return None
    city = _norm(loc.get("city"))
    region = _norm(loc.get("region"))
    country = None
    c = loc.get("country")
    if isinstance(c, dict):
        country = _norm(c.get("localizationValue")) or _norm(c.get("countryCode"))
    parts = [p for p in (city, region, country) if p]
    return ", ".join(parts) if parts else None


def _apollo_work_model(remote_options: Any) -> Optional[str]:
    if not isinstance(remote_options, list):
        return None
    opts = [str(x).upper() for x in remote_options if x]
    if any("HYBRID" in x for x in opts):
        return "Hybrid"
    if any("REMOTE" in x for x in opts) and not any("NON_REMOTE" in x for x in opts):
        return "Remote"
    if any("NON_REMOTE" in x for x in opts):
        return "On-site"
    return None


def _apollo_salary_text(salary: Any) -> Optional[str]:
    if not isinstance(salary, dict):
        return None
    for k in ("displayValue", "formatted", "text", "label", "valueText"):
        v = salary.get(k)
        if isinstance(v, str):
            n = _norm(v)
            if n:
                return n
    return None


def _stringify_job_location(job_location: Any) -> Optional[str]:
    def _one(loc: dict[str, Any]) -> Optional[str]:
        addr = loc.get("address") if isinstance(loc, dict) else None
        if not isinstance(addr, dict):
            return None
        locality = _norm(addr.get("addressLocality"))
        region = _norm(addr.get("addressRegion"))
        country = _norm(addr.get("addressCountry"))
        parts = [p for p in (locality, region, country) if p]
        return ", ".join(parts) if parts else None

    if isinstance(job_location, dict):
        return _one(job_location)
    if isinstance(job_location, list):
        vals = [x for x in (_one(x) for x in job_location if isinstance(x, dict)) if x]
        if vals:
            return vals[0]
    return None


def _extract_highlights(sel: Selector) -> list[str]:
    out: list[str] = []
    items = sel.css('ul[aria-label*="Main details"] li, ul[aria-label*="Hauptdetails"] li')
    for it in items:
        t = it.css("[aria-hidden='true']::text").get()
        if t is None:
            t = it.xpath("normalize-space(string(.))").get()
        n = _norm(t)
        if n:
            out.append(n)
    return out


def _classify_highlights(highlights: list[str]) -> tuple[Optional[str], Optional[str], Optional[str], bool]:
    employment_type: Optional[str] = None
    salary_range_text: Optional[str] = None
    work_model: Optional[str] = None
    external_marker = False

    for h in highlights:
        hl = h.lower()
        if "external job ad" in hl:
            external_marker = True
            continue
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
        if employment_type is None and hl not in _BADGE_TOKENS:
            employment_type = h

    return employment_type, salary_range_text, work_model, external_marker


def _parse_dt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except Exception:
        return _norm(value)


def parse_job_detail(html: str) -> dict[str, Any]:
    sel = Selector(text=html)
    jobposting = _parse_jobposting_json_ld(sel)
    apollo_job, apollo_meta = _parse_runtime_config_apollo(sel)

    jp_title = _norm(jobposting.get("title")) if isinstance(jobposting, dict) else None

    jp_company = None
    if isinstance(jobposting, dict):
        ho = jobposting.get("hiringOrganization")
        if isinstance(ho, dict):
            jp_company = _norm(ho.get("name"))

    jp_location = _stringify_job_location(jobposting.get("jobLocation") if isinstance(jobposting, dict) else None)
    jp_employment_type = _norm(jobposting.get("employmentType")) if isinstance(jobposting, dict) else None
    jp_date_posted = _parse_dt(jobposting.get("datePosted") if isinstance(jobposting, dict) else None)

    jp_desc_raw = None
    if isinstance(jobposting, dict):
        jp_desc_raw = jobposting.get("description")
    jp_desc_html = _norm(unescape(jp_desc_raw)) if isinstance(jp_desc_raw, str) else None
    jp_description = _html_fragment_to_text(jp_desc_html)

    dom_title = _text(sel.css("h1, [data-testid='job-details-title']"))
    dom_company = _text(sel.css("[data-testid='job-details-company-info-name']"))
    dom_location = _text(sel.css("div[class*='multi-location-display'] p"))

    dom_posted_at = _parse_dt(_norm(sel.css("[data-testid='job-details-published-date'] time::attr(datetime), [data-testid='job-details-published-date'] time::attr(dateTime)").get()))
    dom_posted_ago = _norm(sel.css("[data-testid='job-details-published-date'] [aria-hidden='true']::text").get())
    if not dom_posted_ago:
        dom_posted_ago = _norm(sel.css("[data-testid='job-details-published-date']::text").get())

    highlights = _extract_highlights(sel)
    h_employment, h_salary, h_work_model, h_external = _classify_highlights(highlights)

    ap_title = _norm(apollo_job.get("title")) if isinstance(apollo_job, dict) else None
    ap_company = None
    ap_location = None
    ap_employment_type = None
    ap_posted_at = None
    ap_description = None
    ap_salary = None
    ap_work_model = None
    ap_external = False

    if isinstance(apollo_job, dict) and isinstance(apollo_meta, dict):
        compinfo = apollo_job.get("companyInfo")
        if isinstance(compinfo, dict):
            ap_company = _norm(compinfo.get("companyNameOverride"))
            if not ap_company:
                comp = _apollo_deref(apollo_meta, compinfo.get("company"))
                if isinstance(comp, dict):
                    ap_company = _norm(comp.get("companyName"))

        ap_location = _stringify_apollo_location(apollo_job.get("location"))

        emp = _apollo_deref(apollo_meta, apollo_job.get("employmentType"))
        if isinstance(emp, dict):
            ap_employment_type = _norm(emp.get("localizationValue"))

        ap_posted_at = _parse_dt(_norm(apollo_job.get("activatedAt")) or _norm(apollo_job.get("refreshedAt")))

        desc = apollo_job.get("description")
        if isinstance(desc, dict):
            content = desc.get("content")
            if isinstance(content, str):
                ap_description = _html_fragment_to_text(_norm(unescape(content)))

        ap_salary = _apollo_salary_text(apollo_job.get("salary"))
        ap_work_model = _apollo_work_model(apollo_job.get("remoteOptions"))
        ap_external = bool(apollo_job.get("redirectsToThirdPartyUrl"))

    # Fallback description from the main job section only if JSON-LD was missing.
    dom_desc = None
    if not jp_description and not ap_description:
        sec = sel.xpath("//h2[normalize-space(.)='About this job' or normalize-space(.)='Über uns' or normalize-space(.)='Stellenbeschreibung']/ancestor::section[1]")
        dom_desc = _text(sec)

    posted_at_utc = jp_date_posted or dom_posted_at or ap_posted_at
    employment_type = jp_employment_type or h_employment or ap_employment_type

    job_title = jp_title or dom_title or ap_title
    company_name = jp_company or dom_company or ap_company
    job_location = jp_location or dom_location or ap_location
    job_description = jp_description or ap_description or dom_desc

    criteria = {
        "highlights": highlights,
        "external_ad": bool(h_external or ap_external),
        "sources": {
            "title": "jsonld" if jp_title else ("dom" if dom_title else ("apollo" if ap_title else None)),
            "company": "jsonld"
            if jp_company
            else ("dom" if dom_company else ("apollo" if ap_company else None)),
            "location": "jsonld"
            if jp_location
            else ("dom" if dom_location else ("apollo" if ap_location else None)),
            "posted_at_utc": "jsonld"
            if jp_date_posted
            else ("dom" if dom_posted_at else ("apollo" if ap_posted_at else None)),
            "employment_type": "jsonld"
            if jp_employment_type
            else ("highlights" if h_employment else ("apollo" if ap_employment_type else None)),
            "description": "jsonld"
            if jp_description
            else ("apollo" if ap_description else ("dom" if dom_desc else None)),
        },
    }

    return {
        "job_title": job_title,
        "company_name": company_name,
        "job_location": job_location,
        "posted_at_utc": posted_at_utc,
        "posted_time_ago": dom_posted_ago,
        "employment_type": employment_type,
        "salary_range_text": h_salary or ap_salary,
        "work_model": h_work_model or ap_work_model,
        "job_description": job_description,
        "criteria": criteria,
    }
