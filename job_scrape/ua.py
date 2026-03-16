"""Centralized User-Agent management for all spiders.

Keep this in sync with a recent stable Chrome release.  Update quarterly.
Both the discovery and detail spiders should use the same value to avoid
presenting mismatched browser versions from a single IP.
"""
from __future__ import annotations

import os

# Chrome 131 on macOS — update this when Chrome ships a new major version.
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def get_user_agent() -> str:
    """Return the User-Agent to use for LinkedIn requests.

    Override via the ``LINKEDIN_GUEST_USER_AGENT`` environment variable.
    """
    ua = (os.getenv("LINKEDIN_GUEST_USER_AGENT") or "").strip()
    return ua or _DEFAULT_USER_AGENT
