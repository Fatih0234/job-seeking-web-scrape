import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import import_discovery, import_discovery_stepstone, import_discovery_xing


class _CaptureCursor:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql: str, params=None) -> None:
        self.sql_calls.append(" ".join(sql.split()).lower())


class _CaptureConn:
    def __init__(self, cursor: _CaptureCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return self._cursor

    def commit(self) -> None:
        return None


class TestDiscoveryReactivation(unittest.TestCase):
    def _run_import(self, module, records: list[dict]) -> list[str]:
        cursor = _CaptureCursor()
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "in.jsonl"
            p.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
            out = io.StringIO()
            with (
                patch.object(module, "connect", return_value=_CaptureConn(cursor)),
                patch.object(module.sys, "argv", ["script.py", str(p)]),
                redirect_stdout(out),
            ):
                module.main()
        return cursor.sql_calls

    def _assert_reactivation_sql(self, sql_calls: list[str], table_prefix: str) -> None:
        inserts = [s for s in sql_calls if f"insert into {table_prefix}" in s]
        self.assertTrue(inserts, f"No insert SQL captured for {table_prefix}")
        q = inserts[0]
        self.assertIn("is_active", q)
        self.assertIn("is_active = true", q)
        self.assertIn("stale_since_at = null", q)
        self.assertIn("expired_at = null", q)
        self.assertIn("expire_reason = null", q)

    def test_linkedin_discovery_reactivates_seen_jobs(self):
        sql_calls = self._run_import(
            import_discovery,
            [
                {
                    "record_type": "page_fetch",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "page_start": 0,
                },
                {
                    "record_type": "job_discovered",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "source": "linkedin",
                    "job_id": "123",
                    "job_url": "https://www.linkedin.com/jobs/view/123/",
                    "rank": 1,
                    "page_start": 0,
                    "scraped_at": "2026-02-12T12:00:00Z",
                },
            ],
        )
        self._assert_reactivation_sql(sql_calls, "job_scrape.jobs")

    def test_stepstone_discovery_reactivates_seen_jobs(self):
        sql_calls = self._run_import(
            import_discovery_stepstone,
            [
                {
                    "record_type": "page_fetch",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "page_start": 1,
                },
                {
                    "record_type": "job_discovered",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "job_id": "abc",
                    "job_url": "https://www.stepstone.de/stellenangebote--abc",
                    "rank": 1,
                    "page_start": 1,
                    "scraped_at": "2026-02-12T12:00:00Z",
                },
            ],
        )
        self._assert_reactivation_sql(sql_calls, "job_scrape.stepstone_jobs")

    def test_xing_discovery_reactivates_seen_jobs(self):
        sql_calls = self._run_import(
            import_discovery_xing,
            [
                {
                    "record_type": "page_fetch",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "page_start": 0,
                },
                {
                    "record_type": "job_discovered",
                    "crawl_run_id": "crawl-1",
                    "search_run_id": "sr-1",
                    "job_id": "150143308",
                    "job_url": "https://www.xing.com/jobs/ulm-role-150143308",
                    "rank": 0,
                    "page_start": 0,
                    "scraped_at": "2026-02-12T12:00:00Z",
                },
            ],
        )
        self._assert_reactivation_sql(sql_calls, "job_scrape.xing_jobs")


if __name__ == "__main__":
    unittest.main()
