from __future__ import annotations

import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from scripts import report_latest_run


class _FakeCursor:
    def __init__(self, fetchone_rows: list[tuple] | None = None) -> None:
        self._fetchone_rows = list(fetchone_rows or [])
        self.queries: list[tuple[str, tuple | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        if self._fetchone_rows:
            return self._fetchone_rows.pop(0)
        return None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def cursor(self):
        return self._cursor


class TestReportLatestRunScope(unittest.TestCase):
    def test_report_run_id_found_uses_explicit_scope(self):
        started_at = datetime(2026, 2, 17, 1, 6, 40, tzinfo=timezone.utc)
        finished_at = datetime(2026, 2, 17, 1, 18, 38, tzinfo=timezone.utc)
        cursor = _FakeCursor(
            fetchone_rows=[
                (
                    "71ca9021-8bea-4639-b022-138bbd2b9656",
                    "github_schedule_last24h",
                    "success",
                    started_at,
                    finished_at,
                    None,
                    {"details": {"status": "success"}},
                )
            ]
        )
        conn = _FakeConn(cursor)

        buf = io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {
                    "REPORT_SOURCE": "xing",
                    "REPORT_RUN_ID": "71ca9021-8bea-4639-b022-138bbd2b9656",
                },
                clear=False,
            ),
            patch("scripts.report_latest_run.connect", return_value=conn),
            patch(
                "scripts.report_latest_run._report_xing",
                return_value={
                    "discovery": {"search_runs_total": 17},
                    "details_overall": {"source": "xing"},
                    "skills_fill": {"parse_ok_total": 1},
                },
            ),
            patch("sys.stdout", new=buf),
        ):
            report_latest_run.main()

        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(payload["report_scope"], "explicit_run_id")
        self.assertEqual(
            payload["latest_crawl_run"]["id"], "71ca9021-8bea-4639-b022-138bbd2b9656"
        )
        self.assertIn("where id = %s", cursor.queries[0][0].lower())

    def test_report_run_id_missing_returns_no_run_for_id(self):
        cursor = _FakeCursor(fetchone_rows=[None])
        conn = _FakeConn(cursor)

        buf = io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {
                    "REPORT_SOURCE": "xing",
                    "REPORT_RUN_ID": "missing-id",
                },
                clear=False,
            ),
            patch("scripts.report_latest_run.connect", return_value=conn),
            patch("sys.stdout", new=buf),
        ):
            report_latest_run.main()

        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(payload["status"], "no_run_for_id")
        self.assertEqual(payload["report_scope"], "explicit_run_id")
        self.assertEqual(payload["requested_run_id"], "missing-id")

    def test_without_report_run_id_keeps_latest_fallback(self):
        started_at = datetime(2026, 2, 17, 1, 6, 40, tzinfo=timezone.utc)
        finished_at = datetime(2026, 2, 17, 1, 18, 38, tzinfo=timezone.utc)
        cursor = _FakeCursor(
            fetchone_rows=[
                (
                    "71ca9021-8bea-4639-b022-138bbd2b9656",
                    "github_schedule_last24h",
                    "success",
                    started_at,
                    finished_at,
                    None,
                    {"details": {"status": "success"}},
                )
            ]
        )
        conn = _FakeConn(cursor)

        buf = io.StringIO()
        with (
            patch.dict("os.environ", {"REPORT_SOURCE": "xing"}, clear=True),
            patch("scripts.report_latest_run.connect", return_value=conn),
            patch(
                "scripts.report_latest_run._report_xing",
                return_value={
                    "discovery": {"search_runs_total": 17},
                    "details_overall": {"source": "xing"},
                    "skills_fill": {"parse_ok_total": 1},
                },
            ),
            patch("sys.stdout", new=buf),
        ):
            report_latest_run.main()

        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(payload["report_scope"], "latest_fallback")
        self.assertIn("order by started_at desc", cursor.queries[0][0].lower())


if __name__ == "__main__":
    unittest.main()
