from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib.parse import quote_plus

import psycopg
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_CONNECT_RETRIES = 3
_CONNECT_BACKOFF_SECS = (1.0, 2.0, 4.0)


def _load_env() -> None:
    # Load local .env if present. Avoid dotenv's find_dotenv() behavior, which
    # can assert in some execution contexts (e.g. python -c / stdin).
    load_dotenv(dotenv_path=Path(".env"), override=False)


def db_url() -> str:
    _load_env()
    url = os.getenv("SUPABASE_DB_URL")
    if url:
        return url

    host = os.getenv("SUPABASE_HOST", "").strip()
    port = os.getenv("SUPABASE_PORT", "5432").strip() or "5432"
    database = os.getenv("SUPABASE_DATABASE", "postgres").strip() or "postgres"
    user = os.getenv("SUPABASE_USER", "").strip()
    password = os.getenv("SUPABASE_PASSWORD", "")
    sslmode = os.getenv("SUPABASE_SSLMODE", "require").strip() or "require"

    if not host or not user or not password:
        raise RuntimeError(
            "Database config missing. Set SUPABASE_DB_URL or "
            "SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD "
            "(optionally SUPABASE_SSLMODE)."
        )

    user_q = quote_plus(user)
    pass_q = quote_plus(password)
    host_q = host  # host shouldn't need quoting
    db_q = quote_plus(database)
    return f"postgresql://{user_q}:{pass_q}@{host_q}:{port}/{db_q}?sslmode={quote_plus(sslmode)}"


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    # PgBouncer transaction-pooling can reuse backend sessions across clients,
    # which breaks psycopg auto-prepared statement names. Disable prepares for
    # compatibility with pooled Supabase connections.
    #
    # Retry with exponential backoff for transient connection failures.
    last_err: Exception | None = None
    for attempt in range(_CONNECT_RETRIES):
        try:
            conn = psycopg.connect(db_url(), prepare_threshold=None)
            break
        except psycopg.OperationalError as e:
            last_err = e
            if attempt < _CONNECT_RETRIES - 1:
                delay = _CONNECT_BACKOFF_SECS[attempt]
                logger.warning(
                    "DB connect attempt %d failed (%s), retrying in %.1fs...",
                    attempt + 1,
                    e,
                    delay,
                )
                time.sleep(delay)
    else:
        raise last_err  # type: ignore[misc]
    try:
        yield conn
    finally:
        conn.close()


def now_utc_iso() -> str:
    # Let Postgres set timestamps where possible; this is for JSON metadata only.
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()
