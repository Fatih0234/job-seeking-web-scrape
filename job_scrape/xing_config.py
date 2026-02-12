from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class XingSearchSpec:
    name: str
    keywords: tuple[str, ...]
    locations: tuple[str, ...] = ()
    city_ids: dict[str, str] | None = None


@dataclass(frozen=True)
class XingConfig:
    searches: tuple[XingSearchSpec, ...]


def _as_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{field} must be a string")


def _as_str_list(value: Any, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return tuple(value)
    raise ValueError(f"{field} must be a list of strings (or a single string)")


def _normalize_keywords(keywords: tuple[str, ...], *, field: str) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for idx, keyword in enumerate(keywords):
        normalized = " ".join(keyword.split())
        if not normalized:
            raise ValueError(f"{field}[{idx}] cannot be empty")
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return tuple(out)


def _as_str_map(value: Any, *, field: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a mapping of string to string")
    out: dict[str, str] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError(f"{field} must be a mapping of string to string")
        out[k] = v
    return out


def load_xing_config(path: str | Path) -> XingConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "xing" not in data:
        raise ValueError("Invalid config: expected top-level 'xing' key")

    root = data["xing"]
    if not isinstance(root, dict):
        raise ValueError("Invalid config: 'xing' must be a mapping")

    searches_raw = root.get("searches", [])
    if not isinstance(searches_raw, list):
        raise ValueError("Invalid config: 'xing.searches' must be a list")

    searches: list[XingSearchSpec] = []
    for i, sr in enumerate(searches_raw):
        if not isinstance(sr, dict):
            raise ValueError(f"Invalid search at index {i}: must be a mapping")

        name = _as_str(sr.get("name", f"search_{i}"), field=f"xing.searches[{i}].name")
        keywords = _normalize_keywords(
            _as_str_list(sr.get("keywords"), field=f"xing.searches[{i}].keywords"),
            field=f"xing.searches[{i}].keywords",
        )
        locations = _as_str_list(sr.get("locations"), field=f"xing.searches[{i}].locations")
        city_ids = _as_str_map(sr.get("city_ids"), field=f"xing.searches[{i}].city_ids")

        if not keywords or any((not k.strip()) for k in keywords):
            raise ValueError(f"Invalid search '{name}': keywords must be a non-empty string or list of strings")
        if any((not loc.strip()) for loc in locations):
            raise ValueError(f"Invalid search '{name}': locations cannot contain empty strings")

        if city_ids and not locations:
            raise ValueError(f"Invalid search '{name}': city_ids requires locations to be set")

        unknown = sorted(k for k in city_ids.keys() if k not in set(locations))
        if unknown:
            raise ValueError(
                f"Invalid search '{name}': city_ids keys must also appear in locations (unknown: {unknown})"
            )

        searches.append(
            XingSearchSpec(
                name=name,
                keywords=keywords,
                locations=locations,
                city_ids=city_ids,
            )
        )

    return XingConfig(searches=tuple(searches))
