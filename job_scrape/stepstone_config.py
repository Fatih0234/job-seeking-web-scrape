from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

from job_scrape.stepstone import normalize_sort


@dataclass(frozen=True)
class StepstoneSearchSpec:
    name: str
    keywords: tuple[str, ...]
    locations: tuple[str, ...]
    country: str = "Germany"
    sort: int = 2
    radius: int = 30
    where_type: str = "autosuggest"
    search_origin: str = "Resultlist_top-search"
    age_days: Optional[int] = None


@dataclass(frozen=True)
class StepstoneConfig:
    searches: tuple[StepstoneSearchSpec, ...]


def _as_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{field} must be a string")


def _as_str_opt(value: Any, *, field: str) -> Optional[str]:
    if value is None:
        return None
    return _as_str(value, field=field)


def _as_str_list(value: Any, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return tuple(value)
    raise ValueError(f"{field} must be a list of strings (or a single string)")


def _as_int(value: Any, *, field: str, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError as e:
            raise ValueError(f"{field} must be an integer") from e
    raise ValueError(f"{field} must be an integer")


def load_stepstone_config(path: str | Path) -> StepstoneConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "stepstone" not in data:
        raise ValueError("Invalid config: expected top-level 'stepstone' key")

    root = data["stepstone"]
    if not isinstance(root, dict):
        raise ValueError("Invalid config: 'stepstone' must be a mapping")

    searches_raw = root.get("searches", [])
    if not isinstance(searches_raw, list):
        raise ValueError("Invalid config: 'stepstone.searches' must be a list")

    searches: list[StepstoneSearchSpec] = []
    for i, sr in enumerate(searches_raw):
        if not isinstance(sr, dict):
            raise ValueError(f"Invalid search at index {i}: must be a mapping")

        name = _as_str(sr.get("name", f"search_{i}"), field=f"stepstone.searches[{i}].name")
        keywords = _as_str_list(sr.get("keywords"), field=f"stepstone.searches[{i}].keywords")
        locations = _as_str_list(sr.get("locations"), field=f"stepstone.searches[{i}].locations")

        if not keywords or any((not k.strip()) for k in keywords):
            raise ValueError(f"Invalid search '{name}': keywords must be a non-empty string or list of strings")
        if not locations or any((not l.strip()) for l in locations):
            raise ValueError(f"Invalid search '{name}': locations must be a non-empty string or list of strings")

        radius = _as_int(sr.get("radius"), field=f"{name}.radius", default=30)
        if radius <= 0:
            raise ValueError(f"{name}.radius must be > 0")

        sort_raw = sr.get("sort", 2)
        sort = normalize_sort(sort_raw)

        where_type = _as_str(sr.get("where_type", "autosuggest"), field=f"{name}.where_type")
        search_origin = _as_str(sr.get("search_origin", "Resultlist_top-search"), field=f"{name}.search_origin")
        country = _as_str(sr.get("country", "Germany"), field=f"{name}.country")
        age_days_raw = sr.get("age_days")
        age_days: Optional[int] = None
        if age_days_raw is not None:
            age_days = _as_int(age_days_raw, field=f"{name}.age_days", default=0)
            if age_days not in {1, 7}:
                raise ValueError(f"{name}.age_days must be one of: 1, 7")

        searches.append(
            StepstoneSearchSpec(
                name=name,
                keywords=keywords,
                locations=locations,
                country=country,
                sort=sort,
                radius=radius,
                where_type=where_type,
                search_origin=search_origin,
                age_days=age_days,
            )
        )

    return StepstoneConfig(searches=tuple(searches))
