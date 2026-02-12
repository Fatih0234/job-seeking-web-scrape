from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence, TypeVar

import requests
from dotenv import load_dotenv

from scripts.db import connect
from scripts.ensure_geocode_schema import ensure_schema


PROVIDER = "geoapify"
ALLOWED_RETRY_STATUSES = ("pending", "no_match", "error")
GEOAPIFY_BATCH_URL = "https://api.geoapify.com/v1/batch/geocode/search"


@dataclass(frozen=True)
class CacheRow:
    id: str
    location_text_raw: str
    location_text_norm: str
    country_scope: str
    attempt_count: int


def normalize_location_text(text: str) -> str:
    # Keep normalization simple and deterministic for cache-key stability.
    x = re.sub(r"\s+", " ", text).strip()
    x = re.sub(r"\s*,\s*", ", ", x)
    return x.lower()


def country_scope_for_platform(platform: str) -> str:
    p = (platform or "").strip().lower()
    if p in ("linkedin", "stepstone"):
        return "de"
    if p == "xing":
        return "de,at,ch"
    return "de,at,ch"


T = TypeVar("T")


def chunked(items: Sequence[T], size: int) -> Iterable[Sequence[T]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def compute_retry_delay_minutes(*, attempt_after: int, base_minutes: int, max_minutes: int) -> int:
    # first failure -> base; second -> base*2; capped at max_minutes
    exp = max(0, attempt_after - 1)
    mins = base_minutes * (2**exp)
    return min(mins, max_minutes)


def unresolved_status(*, attempt_after: int, max_attempts: int, kind: str) -> str:
    if attempt_after >= max_attempts:
        return "failed_permanent"
    if kind == "error":
        return "error"
    return "no_match"


def extract_query_text(item: dict[str, Any]) -> str | None:
    q = item.get("query")
    if isinstance(q, dict):
        v = q.get("text") or q.get("query")
        return str(v) if v else None
    if isinstance(q, str):
        return q
    return None


def build_result_map(results: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in results:
        q = extract_query_text(item)
        if not q:
            continue
        out[normalize_location_text(q)] = item
    return out


def parse_batch_payload(payload: Any) -> tuple[str, list[dict[str, Any]], str | None]:
    if isinstance(payload, list):
        rows = [x for x in payload if isinstance(x, dict)]
        return ("finished", rows, None)
    if isinstance(payload, dict):
        status = str(payload.get("status") or "").lower()
        if status == "pending":
            return ("pending", [], None)
        if status == "finished":
            rows = payload.get("results")
            if isinstance(rows, list):
                rows = [x for x in rows if isinstance(x, dict)]
                return ("finished", rows, None)
            return ("finished", [], None)
        if status:
            return (status, [], json.dumps(payload, ensure_ascii=False))
    return ("error", [], f"unexpected_payload_type={type(payload).__name__}")


def poll_batch_results(
    *,
    session: requests.Session,
    api_key: str,
    job_id: str,
    poll_seconds: int,
    poll_timeout_seconds: int,
) -> list[dict[str, Any]]:
    started = time.monotonic()
    while True:
        resp = session.get(
            GEOAPIFY_BATCH_URL,
            params={"apiKey": api_key, "id": job_id},
            timeout=60,
        )
        resp.raise_for_status()
        status, results, err = parse_batch_payload(resp.json())
        if status == "finished":
            return results
        if status != "pending":
            raise RuntimeError(f"Geoapify polling returned status={status}; err={err}")
        if time.monotonic() - started >= poll_timeout_seconds:
            raise TimeoutError(f"Geoapify polling timeout for job_id={job_id}")
        time.sleep(poll_seconds)


def create_batch_job(
    *,
    session: requests.Session,
    api_key: str,
    country_scope: str,
    addresses: list[str],
) -> tuple[str | None, list[dict[str, Any]] | None]:
    resp = session.post(
        GEOAPIFY_BATCH_URL,
        params={
            "apiKey": api_key,
            "filter": f"countrycode:{country_scope}",
            "limit": 1,
            "format": "json",
        },
        json=addresses,
        timeout=60,
    )
    resp.raise_for_status()

    status, results, err = parse_batch_payload(resp.json())
    if status == "finished":
        return (None, results)
    if status == "pending":
        body = resp.json()
        if isinstance(body, dict):
            job_id = body.get("id")
            if isinstance(job_id, str) and job_id:
                return (job_id, None)
        raise RuntimeError("Geoapify batch create returned pending without a valid job id")
    raise RuntimeError(f"Geoapify batch create failed with status={status}; err={err}")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _load_env() -> None:
    load_dotenv(dotenv_path=".env", override=False)


def _seed_cache(cur) -> int:
    cur.execute(
        """
        with src as (
          select distinct
            nullif(btrim(city_token_raw), '') as location_text_raw,
            nullif(btrim(city_token_norm), '') as location_text_norm,
            country_scope
          from job_scrape.jobs_dashboard_location_candidates_v
          where token_kind = 'city'
            and nullif(btrim(city_token_norm), '') is not null
        ),
        ins as (
          insert into job_scrape.location_geocode_cache
            (provider, location_text_raw, location_text_norm, country_scope, status, next_retry_at)
          select
            %s,
            location_text_raw,
            location_text_norm,
            country_scope,
            'pending',
            now()
          from src
          on conflict (provider, location_text_norm, country_scope) do nothing
          returning 1
        )
        select count(*) from ins
        """,
        (PROVIDER,),
    )
    return int(cur.fetchone()[0] or 0)


def _select_work(cur, *, max_rows: int, max_attempts: int) -> list[CacheRow]:
    cur.execute(
        """
        select id, location_text_raw, location_text_norm, country_scope, attempt_count
        from job_scrape.location_geocode_cache
        where provider = %s
          and status = any(%s)
          and next_retry_at <= now()
          and attempt_count < %s
        order by next_retry_at asc, id asc
        limit %s
        """,
        (PROVIDER, list(ALLOWED_RETRY_STATUSES), max_attempts, max_rows),
    )
    return [
        CacheRow(
            id=str(r[0]),
            location_text_raw=str(r[1]),
            location_text_norm=str(r[2]),
            country_scope=str(r[3]),
            attempt_count=int(r[4] or 0),
        )
        for r in cur.fetchall()
    ]


def _update_row(
    cur,
    *,
    row_id: str,
    status: str,
    attempt_after: int,
    next_retry_at: datetime,
    last_error: str | None,
    raw_response: dict[str, Any] | None,
    resolved_at: datetime | None,
    lat: float | None,
    lon: float | None,
    formatted: str | None,
    result_type: str | None,
    rank_confidence: float | None,
    rank_importance: float | None,
    city: str | None,
    state: str | None,
    country: str | None,
    country_code: str | None,
    timezone_name: str | None,
    query_text_returned: str | None,
) -> None:
    cur.execute(
        """
        update job_scrape.location_geocode_cache
           set status = %s,
               attempt_count = %s,
               last_attempted_at = now(),
               next_retry_at = %s,
               resolved_at = %s,
               lat = %s,
               lon = %s,
               formatted = %s,
               result_type = %s,
               rank_confidence = %s,
               rank_importance = %s,
               city = %s,
               state = %s,
               country = %s,
               country_code = %s,
               timezone_name = %s,
               query_text_returned = %s,
               raw_response = %s::jsonb,
               last_error = %s,
               updated_at = now()
         where id = %s
        """,
        (
            status,
            attempt_after,
            next_retry_at,
            resolved_at,
            lat,
            lon,
            formatted,
            result_type,
            rank_confidence,
            rank_importance,
            city,
            state,
            country,
            country_code,
            timezone_name,
            query_text_returned,
            _json(raw_response) if raw_response is not None else None,
            last_error,
            row_id,
        ),
    )


def _resolve_result_for_row(
    *,
    row: CacheRow,
    idx: int,
    rows_in_batch: Sequence[CacheRow],
    results: Sequence[dict[str, Any]],
    result_map: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    # primary: map by normalized query text; fallback: positional mapping.
    item = result_map.get(row.location_text_norm)
    if item is not None:
        return item
    if len(results) == len(rows_in_batch):
        cand = results[idx]
        if isinstance(cand, dict):
            return cand
    return None


def _safe_float(x: Any) -> float | None:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _run() -> dict[str, Any]:
    started = time.monotonic()
    _load_env()
    api_key = (os.getenv("GEOAPIFY_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GEOAPIFY_API_KEY is required")

    max_rows = int(os.getenv("GEOCODE_MAX_LOCATIONS_PER_RUN", "3000"))
    batch_size = int(os.getenv("GEOAPIFY_BATCH_SIZE", "1000"))
    poll_seconds = int(os.getenv("GEOAPIFY_POLL_SECONDS", "2"))
    poll_timeout_seconds = int(os.getenv("GEOAPIFY_POLL_TIMEOUT_SECONDS", "300"))
    max_attempts = int(os.getenv("GEOCODE_MAX_ATTEMPTS", "6"))
    retry_base_minutes = int(os.getenv("GEOCODE_RETRY_BASE_MINUTES", "60"))
    retry_max_minutes = int(os.getenv("GEOCODE_RETRY_MAX_MINUTES", "10080"))

    if batch_size <= 0:
        raise ValueError("GEOAPIFY_BATCH_SIZE must be > 0")
    if batch_size > 1000:
        batch_size = 1000

    if os.getenv("GEOCODE_ENSURE_SCHEMA", "1") == "1":
        ensure_schema()

    seeded_count = 0
    attempted_count = 0
    resolved_count = 0
    no_match_count = 0
    error_count = 0
    permanent_failures = 0
    scope_counts: dict[str, int] = defaultdict(int)

    with connect() as conn:
        with conn.cursor() as cur:
            if os.getenv("GEOCODE_SEED_CACHE", "1") == "1":
                seeded_count = _seed_cache(cur)
            rows = _select_work(cur, max_rows=max_rows, max_attempts=max_attempts)
            conn.commit()

        if not rows:
            elapsed = round(time.monotonic() - started, 3)
            return {
                "status": "success",
                "seeded_count": seeded_count,
                "attempted_count": 0,
                "resolved_count": 0,
                "no_match_count": 0,
                "error_count": 0,
                "permanent_failures": 0,
                "scope_counts": {},
                "elapsed_seconds": elapsed,
            }

        by_scope: dict[str, list[CacheRow]] = defaultdict(list)
        for r in rows:
            by_scope[r.country_scope].append(r)

        session = requests.Session()
        with conn.cursor() as cur:
            for scope, scope_rows in by_scope.items():
                for batch_rows in chunked(scope_rows, batch_size):
                    batch_rows = list(batch_rows)
                    if not batch_rows:
                        continue

                    attempted_count += len(batch_rows)
                    scope_counts[scope] += len(batch_rows)

                    try:
                        addresses = [r.location_text_raw for r in batch_rows]
                        job_id, results_immediate = create_batch_job(
                            session=session,
                            api_key=api_key,
                            country_scope=scope,
                            addresses=addresses,
                        )
                        if results_immediate is not None:
                            results = results_immediate
                        else:
                            results = poll_batch_results(
                                session=session,
                                api_key=api_key,
                                job_id=str(job_id),
                                poll_seconds=poll_seconds,
                                poll_timeout_seconds=poll_timeout_seconds,
                            )

                        result_map = build_result_map(results)
                        now_utc = _now_utc()

                        for idx, row in enumerate(batch_rows):
                            attempt_after = row.attempt_count + 1
                            item = _resolve_result_for_row(
                                row=row,
                                idx=idx,
                                rows_in_batch=batch_rows,
                                results=results,
                                result_map=result_map,
                            )

                            lat = _safe_float(item.get("lat")) if item else None
                            lon = _safe_float(item.get("lon")) if item else None
                            is_resolved = lat is not None and lon is not None

                            if is_resolved:
                                _update_row(
                                    cur,
                                    row_id=row.id,
                                    status="resolved",
                                    attempt_after=attempt_after,
                                    next_retry_at=now_utc,
                                    last_error=None,
                                    raw_response=item,
                                    resolved_at=now_utc,
                                    lat=lat,
                                    lon=lon,
                                    formatted=item.get("formatted"),
                                    result_type=item.get("result_type"),
                                    rank_confidence=_safe_float((item.get("rank") or {}).get("confidence")),
                                    rank_importance=_safe_float((item.get("rank") or {}).get("importance")),
                                    city=item.get("city"),
                                    state=item.get("state"),
                                    country=item.get("country"),
                                    country_code=item.get("country_code"),
                                    timezone_name=(item.get("timezone") or {}).get("name")
                                    if isinstance(item.get("timezone"), dict)
                                    else None,
                                    query_text_returned=extract_query_text(item),
                                )
                                resolved_count += 1
                                continue

                            # unresolved match: no result or no coordinates
                            delay_mins = compute_retry_delay_minutes(
                                attempt_after=attempt_after,
                                base_minutes=retry_base_minutes,
                                max_minutes=retry_max_minutes,
                            )
                            next_retry_at = now_utc + timedelta(minutes=delay_mins)
                            status = unresolved_status(
                                attempt_after=attempt_after,
                                max_attempts=max_attempts,
                                kind="no_match",
                            )
                            if status == "failed_permanent":
                                permanent_failures += 1
                            else:
                                no_match_count += 1

                            _update_row(
                                cur,
                                row_id=row.id,
                                status=status,
                                attempt_after=attempt_after,
                                next_retry_at=next_retry_at,
                                last_error="no_match_or_missing_coordinates",
                                raw_response=item,
                                resolved_at=None,
                                lat=None,
                                lon=None,
                                formatted=None,
                                result_type=None,
                                rank_confidence=None,
                                rank_importance=None,
                                city=None,
                                state=None,
                                country=None,
                                country_code=None,
                                timezone_name=None,
                                query_text_returned=extract_query_text(item) if item else None,
                            )

                    except Exception as e:
                        # Batch-level failure: mark all rows in this batch with retry/backoff.
                        now_utc = _now_utc()
                        for row in batch_rows:
                            attempt_after = row.attempt_count + 1
                            delay_mins = compute_retry_delay_minutes(
                                attempt_after=attempt_after,
                                base_minutes=retry_base_minutes,
                                max_minutes=retry_max_minutes,
                            )
                            next_retry_at = now_utc + timedelta(minutes=delay_mins)
                            status = unresolved_status(
                                attempt_after=attempt_after,
                                max_attempts=max_attempts,
                                kind="error",
                            )
                            if status == "failed_permanent":
                                permanent_failures += 1
                            else:
                                error_count += 1

                            _update_row(
                                cur,
                                row_id=row.id,
                                status=status,
                                attempt_after=attempt_after,
                                next_retry_at=next_retry_at,
                                last_error=f"batch_error:{type(e).__name__}:{e}",
                                raw_response=None,
                                resolved_at=None,
                                lat=None,
                                lon=None,
                                formatted=None,
                                result_type=None,
                                rank_confidence=None,
                                rank_importance=None,
                                city=None,
                                state=None,
                                country=None,
                                country_code=None,
                                timezone_name=None,
                                query_text_returned=None,
                            )

            conn.commit()

    elapsed = round(time.monotonic() - started, 3)
    return {
        "status": "success",
        "seeded_count": seeded_count,
        "attempted_count": attempted_count,
        "resolved_count": resolved_count,
        "no_match_count": no_match_count,
        "error_count": error_count,
        "permanent_failures": permanent_failures,
        "scope_counts": dict(scope_counts),
        "elapsed_seconds": elapsed,
    }


def main() -> None:
    out = _run()
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
