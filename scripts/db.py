from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib.parse import quote_plus

import psycopg
from dotenv import load_dotenv


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
    conn = psycopg.connect(db_url())
    try:
        yield conn
    finally:
        conn.close()


def now_utc_iso() -> str:
    # Let Postgres set timestamps where possible; this is for JSON metadata only.
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()
