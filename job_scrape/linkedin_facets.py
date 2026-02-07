from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from parsel import Selector


@dataclass(frozen=True)
class FacetOption:
    value: str
    label: str


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()).lower()


def parse_facet_options(html: str) -> dict[str, list[FacetOption]]:
    """
    Parse the guest jobs search HTML and extract filter option labels + values.
    Returns a mapping from query param name (e.g. 'f_JT') to options.
    """
    sel = Selector(text=html)
    options: dict[str, list[FacetOption]] = {}

    # Inputs are in a form with labels pointing to input ids: <label for="f_JT-0">Full-time (12,345)</label>
    for inp in sel.css("input[form='jserp-filters'][name]"):
        name = inp.attrib.get("name")
        if not name:
            continue
        if name not in {"f_JT", "f_E", "f_WT", "f_TPR"}:
            continue
        _id = inp.attrib.get("id")
        value = inp.attrib.get("value", "")
        label = ""
        if _id:
            label_bits = sel.css(f"label[for='{_id}']::text").getall()
            label = " ".join(t.strip() for t in label_bits if t.strip())

        # Strip counts suffix: "Full-time (12,345)" -> "Full-time"
        label = re.sub(r"\s*\(.*\)\s*$", "", label).strip()

        # f_TPR "Any time" uses an empty value. Keep it.
        if name != "f_TPR" and (not value or not label):
            continue

        options.setdefault(name, []).append(FacetOption(value=value, label=label))

    return options


def build_label_to_value_map(options: dict[str, list[FacetOption]]) -> dict[str, dict[str, str]]:
    """
    Normalize labels and map them to values: map['f_JT']['full-time'] -> 'F'
    """
    out: dict[str, dict[str, str]] = {}
    for facet, opts in options.items():
        m: dict[str, str] = {}
        for opt in opts:
            if not opt.label and facet != "f_TPR":
                continue
            m[_norm_label(opt.label)] = opt.value
        out[facet] = m
    return out


def resolve_facet_values(
    label_to_value: dict[str, dict[str, str]],
    *,
    facet: str,
    requested_labels: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """
    Resolve user-facing labels to LinkedIn facet values. If the user passes a
    raw value (like 'F' or 'r604800'), accept it.
    """
    if not requested_labels:
        return []

    resolved: list[str] = []
    lookup = label_to_value.get(facet, {})
    for raw in requested_labels:
        s = str(raw).strip()
        if not s:
            continue
        # Accept raw code values.
        if facet == "f_TPR":
            # r2592000 etc, or empty string for Any time
            if s.startswith("r") and s[1:].isdigit():
                resolved.append(s)
                continue
        else:
            if len(s) <= 4 and re.fullmatch(r"[A-Za-z0-9]+", s):
                # 'F', 'P', 'C', '1', '2', ...
                resolved.append(s)
                continue

        key = _norm_label(s)
        if key in lookup:
            resolved.append(lookup[key])
        else:
            raise ValueError(f"Unknown {facet} label '{raw}'. Known: {sorted(lookup.keys())}")

    return resolved

