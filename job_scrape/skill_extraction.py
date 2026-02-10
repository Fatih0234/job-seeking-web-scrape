from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


_DEFAULT_TAXONOMY_PATH = Path("configs") / "data-engineering-keyword-taxonomy.yaml"
_LEGACY_TAXONOMY_PATH = Path("data-engineering-keyword-taxonomoy.yaml")
_ALNUM_RE = re.compile(r"[A-Za-z0-9]")


def _clean_str(v: Any) -> str:
    if not isinstance(v, str):
        raise ValueError("expected string")
    s = " ".join(v.split()).strip()
    if not s:
        raise ValueError("empty string")
    return s


def _normalize_for_match(v: str) -> str:
    # Deterministic normalization: unicode NFKC, lowercase, collapse whitespace.
    s = unicodedata.normalize("NFKC", v)
    s = s.lower()
    s = " ".join(s.split()).strip()
    return s


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        key = v.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _flatten_aliases(aliases_raw: Any, *, ctx: str) -> list[str]:
    """
    Backward compatible aliases parsing:
    - aliases: ["..."] (old format)
    - aliases: { en: ["..."], de: ["..."], ... } (new format)
    Returns a flat list of strings (may include duplicates; caller dedupes).
    """
    if aliases_raw is None:
        return []

    if isinstance(aliases_raw, list):
        if not all(isinstance(x, str) for x in aliases_raw):
            raise ValueError(f"{ctx}.aliases must be a list of strings")
        return list(aliases_raw)

    if isinstance(aliases_raw, dict):
        out: list[str] = []
        for lang, vals in aliases_raw.items():
            if not isinstance(vals, list) or not all(isinstance(x, str) for x in vals):
                raise ValueError(f"{ctx}.aliases[{lang!r}] must be a list of strings")
            out.extend(vals)
        return out

    raise ValueError(f"{ctx}.aliases must be a list of strings or a mapping of language->list[str]")


def _compile_alias_regex(aliases: list[str]) -> Optional[re.Pattern[str]]:
    # Avoid very short purely-alpha aliases (e.g. "go", "sh") unless you have
    # special handling; otherwise they cause many false positives.
    cleaned: list[str] = []
    for a in aliases:
        if not isinstance(a, str):
            continue
        a = _normalize_for_match(a)
        if not a:
            continue
        if a.isalpha() and len(a) <= 2:
            continue
        cleaned.append(a)

    cleaned = _dedupe_preserve_order(cleaned)
    if not cleaned:
        return None

    # Longer first to reduce regex backtracking on common prefixes.
    cleaned.sort(key=len, reverse=True)
    parts = "|".join(re.escape(x) for x in cleaned)
    # "Word boundary" for skills: only enforce boundaries on alnum to handle
    # ".NET", "C#", "pl/sql", etc.
    pattern = rf"(?<![A-Za-z0-9])(?:{parts})(?![A-Za-z0-9])"
    return re.compile(pattern, flags=re.IGNORECASE)


@dataclass(frozen=True)
class SkillEntry:
    canonical: str
    aliases: tuple[str, ...]
    _rx: Optional[re.Pattern[str]]

    def matches(self, text: str) -> bool:
        if not text:
            return False
        if not self._rx:
            return False
        return self._rx.search(text) is not None


@dataclass(frozen=True)
class SkillTaxonomy:
    version: int
    groups: dict[str, tuple[SkillEntry, ...]]


def load_skill_taxonomy(path: str | Path = _DEFAULT_TAXONOMY_PATH) -> SkillTaxonomy:
    # Allow overriding via env var to make scripts robust to different working dirs.
    import os

    env_path = os.getenv("SKILL_TAXONOMY_PATH")
    if env_path:
        p = Path(env_path)
    else:
        p = Path(path)
        if not p.exists() and _LEGACY_TAXONOMY_PATH.exists():
            p = _LEGACY_TAXONOMY_PATH

    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("taxonomy must be a YAML mapping")

    version_raw = data.get("version", 1)
    try:
        version = int(version_raw)
    except Exception as e:
        raise ValueError(f"taxonomy.version must be int, got {version_raw!r}") from e

    groups_raw = data.get("groups")
    if not isinstance(groups_raw, dict) or not groups_raw:
        raise ValueError("taxonomy.groups must be a non-empty mapping")

    groups: dict[str, tuple[SkillEntry, ...]] = {}
    for group_name, items_raw in groups_raw.items():
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("group name must be a non-empty string")
        if not isinstance(items_raw, list):
            raise ValueError(f"group {group_name!r} must be a list")

        entries: list[SkillEntry] = []
        for idx, item in enumerate(items_raw):
            if not isinstance(item, dict):
                raise ValueError(f"group {group_name!r} item[{idx}] must be a mapping")
            canonical = _clean_str(item.get("canonical"))
            aliases_flat = _flatten_aliases(item.get("aliases"), ctx=f"group {group_name!r} item[{idx}]")

            aliases: list[str] = []
            for x in aliases_flat:
                # Safeguard: ignore empty/whitespace aliases.
                if not isinstance(x, str):
                    continue
                x2 = _normalize_for_match(x)
                if not x2:
                    continue
                aliases.append(x2)

            # Also match canonical spelling (still stored as-is for output).
            aliases.append(_normalize_for_match(canonical))

            rx = _compile_alias_regex(aliases)
            entries.append(SkillEntry(canonical=canonical, aliases=tuple(_dedupe_preserve_order(aliases)), _rx=rx))

        groups[group_name.strip()] = tuple(entries)

    return SkillTaxonomy(version=version, groups=groups)


def extract_grouped_skills(text: Optional[str], *, taxonomy: SkillTaxonomy) -> dict[str, list[str]]:
    """
    Deterministically extract canonical skill names per group.
    Output lists preserve YAML canonical order per group.
    """
    if not text:
        return {}

    text_n = _normalize_for_match(text)
    if not text_n:
        return {}

    out: dict[str, list[str]] = {}
    for group_name, entries in taxonomy.groups.items():
        hits: list[str] = []
        for e in entries:
            if e.matches(text_n):
                hits.append(e.canonical)
        if hits:
            out[group_name] = hits
    return out
