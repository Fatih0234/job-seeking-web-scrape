from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg
from dotenv import load_dotenv


def _load_env() -> None:
    # Load local .env if present (no-op in CI unless .env is provided).
    load_dotenv(override=False)


def db_url() -> str:
    _load_env()
    url = os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("SUPABASE_DB_URL is required (see .env.example)")
    return url


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(db_url())
    try:
        yield conn
    finally:
        conn.close()


def now_utc_iso() -> str:
    # Let Postgres set timestamps where possible; this is for JSON metadata only.
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()

