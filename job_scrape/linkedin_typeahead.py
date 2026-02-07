from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode


TYPEAHEAD_BASE = "https://www.linkedin.com/jobs-guest/api/typeaheadHits"


@dataclass(frozen=True)
class TypeaheadHit:
    id: str
    display_name: str
    type: str


def build_typeahead_url(*, geo_types: str, query: str) -> str:
    params = {
        "typeaheadType": "GEO",
        "geoTypes": geo_types,
        "query": query,
    }
    return f"{TYPEAHEAD_BASE}?{urlencode(params)}"


class JsonFileCache:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, data: dict) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)


def pick_best_geo_hit(hits: list[dict], *, prefer_suffix: Optional[str] = None) -> Optional[TypeaheadHit]:
    """
    Prefer a hit whose displayName ends with prefer_suffix (case-insensitive).
    Otherwise return the first hit.
    """
    if not hits:
        return None

    def to_hit(h: dict) -> TypeaheadHit:
        return TypeaheadHit(id=str(h.get("id", "")), display_name=str(h.get("displayName", "")), type=str(h.get("type", "")))

    if prefer_suffix:
        suf = prefer_suffix.strip().lower()
        for h in hits:
            dn = str(h.get("displayName", "")).strip().lower()
            if dn.endswith(suf):
                return to_hit(h)

    return to_hit(hits[0])

