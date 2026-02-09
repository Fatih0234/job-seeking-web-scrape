from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def normalize_facets(facets: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a LinkedIn facets dict before using it in URL generation.

    Rules:
    - Drop keys whose values are None
    - Treat f_TPR="" as "Any time" and omit it (so it doesn't appear in the URL)
    """
    out: dict[str, Any] = {}
    for k, v in (facets or {}).items():
        if v is None:
            continue
        if k == "f_TPR" and str(v) == "":
            continue
        out[k] = v
    return out


def _to_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def apply_auto_tpr_if_any_time(
    *,
    facets: dict[str, Any],
    has_finished_history: bool,
    last_success_finished_at: Optional[datetime],
    now_utc: datetime,
    recent_hours: float = 30,
    recent_code: str = "r86400",
    fallback_code: str = "r604800",
) -> dict[str, Any]:
    """
    If the configured facets are effectively "Any time" (f_TPR omitted), choose a
    time window based on past run history:
    - no finished history: keep Any time (backfill)
    - recent success within recent_hours: Past 24 hours (r86400 by default)
    - otherwise: Past week (r604800 by default)
    """
    f = normalize_facets(facets)

    # Respect explicit non-empty f_TPR (static window).
    existing = f.get("f_TPR")
    if isinstance(existing, str) and existing != "":
        return f

    if not has_finished_history:
        # First-ever (or never-finished) run: do not constrain by date.
        f.pop("f_TPR", None)
        return f

    if last_success_finished_at is None:
        f["f_TPR"] = fallback_code
        return f

    now_utc = _to_aware_utc(now_utc)
    last_success_finished_at = _to_aware_utc(last_success_finished_at)
    delta_h = (now_utc - last_success_finished_at).total_seconds() / 3600.0

    if delta_h <= float(recent_hours):
        f["f_TPR"] = recent_code
    else:
        f["f_TPR"] = fallback_code
    return f

