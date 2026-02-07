from __future__ import annotations

import os


def _int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def budgets() -> dict[str, int]:
    """
    Runtime budgets to keep crawls safe.
    Configure via env vars.
    """
    return {
        "MAX_PAGES_PER_SEARCH": _int_env("MAX_PAGES_PER_SEARCH", 50),
        "MAX_JOBS_DISCOVERED_PER_SEARCH": _int_env("MAX_JOBS_DISCOVERED_PER_SEARCH", 2000),
        "MAX_JOB_DETAILS_PER_RUN": _int_env("MAX_JOB_DETAILS_PER_RUN", 200),
        "CIRCUIT_BREAKER_BLOCKS": _int_env("CIRCUIT_BREAKER_BLOCKS", 3),
        "DUPLICATE_PAGE_LIMIT": _int_env("DUPLICATE_PAGE_LIMIT", 3),
        "DETAIL_STALENESS_DAYS": _int_env("DETAIL_STALENESS_DAYS", 7),
        "DETAIL_DEBUG_FAILURE_LIMIT": _int_env("DETAIL_DEBUG_FAILURE_LIMIT", 5),
    }

