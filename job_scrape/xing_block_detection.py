from __future__ import annotations


_BLOCK_STATUSES = {403, 429, 503}
_BLOCK_SUBSTRINGS = (
    "access denied",
    "verify you are a human",
    "captcha",
    "temporarily blocked",
    "errors.edgesuite.net",
)


def looks_blocked(*, status: int, body: str) -> bool:
    if status in _BLOCK_STATUSES:
        return True
    body_l = (body or "").lower()
    return any(s in body_l for s in _BLOCK_SUBSTRINGS)
