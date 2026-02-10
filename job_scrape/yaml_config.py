from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class CountrySpec:
    name: str
    geo_id: Optional[str] = None
    location: Optional[str] = None
    cities_mode: str = "country_only"  # country_only | list
    cities: tuple[str, ...] = ()


@dataclass(frozen=True)
class LinkedInFiltersSpec:
    date_posted: Optional[str] = None
    job_type: tuple[str, ...] = ()
    experience_level: tuple[str, ...] = ()
    remote: tuple[str, ...] = ()


@dataclass(frozen=True)
class LinkedInSearchSpec:
    name: str
    keywords: tuple[str, ...]
    countries: tuple[CountrySpec, ...]
    filters: LinkedInFiltersSpec


@dataclass(frozen=True)
class LinkedInConfig:
    searches: tuple[LinkedInSearchSpec, ...]


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
        # Allow a single scalar string and treat it as a 1-item list.
        return (value,)
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return tuple(value)
    raise ValueError(f"{field} must be a list of strings (or a single string)")


def load_linkedin_config(path: str | Path) -> LinkedInConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "linkedin" not in data:
        raise ValueError("Invalid config: expected top-level 'linkedin' key")

    root = data["linkedin"]
    if not isinstance(root, dict):
        raise ValueError("Invalid config: 'linkedin' must be a mapping")

    searches_raw = root.get("searches", [])
    if not isinstance(searches_raw, list):
        raise ValueError("Invalid config: 'linkedin.searches' must be a list")

    searches: list[LinkedInSearchSpec] = []
    for i, sr in enumerate(searches_raw):
        if not isinstance(sr, dict):
            raise ValueError(f"Invalid search at index {i}: must be a mapping")

        name = _as_str(sr.get("name", f"search_{i}"), field=f"linkedin.searches[{i}].name")
        keywords = _as_str_list(sr.get("keywords", None), field=f"linkedin.searches[{i}].keywords")
        if not keywords or any((not k.strip()) for k in keywords):
            raise ValueError(f"Invalid search '{name}': keywords must be a non-empty string or list of strings")

        countries_raw = sr.get("countries", [])
        if not isinstance(countries_raw, list) or not countries_raw:
            raise ValueError(f"Invalid search '{name}': countries must be a non-empty list")

        countries: list[CountrySpec] = []
        for j, cr in enumerate(countries_raw):
            if not isinstance(cr, dict):
                raise ValueError(f"Invalid country at index {j} in search '{name}': must be a mapping")
            cname = _as_str(cr.get("name"), field=f"countries[{j}].name")
            geo_id = _as_str_opt(cr.get("geo_id"), field=f"countries[{j}].geo_id")
            location = _as_str_opt(cr.get("location"), field=f"countries[{j}].location")
            cities_raw = cr.get("cities") or {}
            cities_mode = "country_only"
            cities: tuple[str, ...] = ()
            if isinstance(cities_raw, dict):
                cities_mode = _as_str(cities_raw.get("mode", "country_only"), field=f"countries[{j}].cities.mode")
                cities = _as_str_list(cities_raw.get("names"), field=f"countries[{j}].cities.names")
            elif cities_raw is None:
                pass
            else:
                raise ValueError(f"countries[{j}].cities must be a mapping (mode/names)")

            countries.append(
                CountrySpec(
                    name=cname,
                    geo_id=geo_id,
                    location=location,
                    cities_mode=cities_mode,
                    cities=cities,
                )
            )

        filters_raw = sr.get("filters") or {}
        if not isinstance(filters_raw, dict):
            raise ValueError(f"Invalid search '{name}': filters must be a mapping")

        filters = LinkedInFiltersSpec(
            date_posted=_as_str_opt(filters_raw.get("date_posted"), field=f"{name}.filters.date_posted"),
            job_type=_as_str_list(filters_raw.get("job_type"), field=f"{name}.filters.job_type"),
            experience_level=_as_str_list(filters_raw.get("experience_level"), field=f"{name}.filters.experience_level"),
            remote=_as_str_list(filters_raw.get("remote"), field=f"{name}.filters.remote"),
        )

        searches.append(
            LinkedInSearchSpec(
                name=name,
                keywords=keywords,
                countries=tuple(countries),
                filters=filters,
            )
        )

    return LinkedInConfig(searches=tuple(searches))
