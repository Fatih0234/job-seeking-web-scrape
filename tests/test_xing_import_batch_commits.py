from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, _sql, _params=None):
        return None

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self) -> None:
        self.commit_calls = 0
        self._cursor = _FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_calls += 1


class _DummyTaxonomy:
    version = 1


class TestXingImportBatchCommits(unittest.TestCase):
    def test_import_discovery_commits_periodically(self):
        from scripts import import_discovery_xing

        rows = []
        for i in range(120):
            rows.append(
                {
                    "record_type": "job_discovered",
                    "crawl_run_id": "crid",
                    "search_run_id": "srid",
                    "job_id": f"jid_{i}",
                    "job_url": f"https://example.com/{i}",
                    "scraped_at": "2026-01-01T00:00:00+00:00",
                    "rank": i,
                    "page_start": 0,
                    "is_external": False,
                    "list_preview": {},
                }
            )

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "in.jsonl"
            p.write_text(
                "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
            )

            fake_conn = _FakeConn()
            with (
                patch("scripts.import_discovery_xing.connect", return_value=fake_conn),
                patch("sys.argv", ["import_discovery_xing.py", str(p)]),
            ):
                import_discovery_xing.main()

        # 120 rows with COMMIT_EVERY=50 => commits at 50, 100, and final commit.
        self.assertEqual(fake_conn.commit_calls, 3)

    def test_import_details_commits_periodically(self):
        from scripts import import_details_xing

        rows = []
        for i in range(120):
            rows.append(
                {
                    "record_type": "job_detail",
                    "crawl_run_id": "crid",
                    "source": "xing",
                    "job_id": f"jid_{i}",
                    "job_url": f"https://example.com/{i}",
                    "scraped_at": "2026-01-01T00:00:00+00:00",
                    "parse_ok": False,
                    "blocked": False,
                    "last_error": "http_500",
                    "criteria": {},
                }
            )

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "in.jsonl"
            p.write_text(
                "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
            )

            fake_conn = _FakeConn()
            with (
                patch("scripts.import_details_xing.connect", return_value=fake_conn),
                patch(
                    "scripts.import_details_xing.load_skill_taxonomy",
                    return_value=_DummyTaxonomy(),
                ),
                patch("sys.argv", ["import_details_xing.py", str(p)]),
            ):
                import_details_xing.main()

        self.assertEqual(fake_conn.commit_calls, 3)


if __name__ == "__main__":
    unittest.main()
