from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from scripts import run_details, run_details_stepstone, run_details_xing


class _CaptureCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return []


class _CaptureConn:
    def __init__(self, cursor: _CaptureCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return self._cursor


class TestDetailsLastSeenGuard(unittest.TestCase):
    def _assert_guard(self, module, source: str | None = None) -> None:
        cursor = _CaptureCursor()
        with patch.object(module, "connect", return_value=_CaptureConn(cursor)):
            module.select_jobs_for_details(
                limit=25,
                staleness_days=7,
                blocked_retry_hours=24,
                last_seen_window_days=60,
            )

        sql_norm = " ".join(cursor.sql.split()).lower()
        self.assertIn("last_seen_at > now() - (%s || ' days')::interval", sql_norm)
        self.assertEqual(cursor.params[0], "60")
        if source is not None:
            self.assertIn("where j.source = 'linkedin'", sql_norm)

    def test_linkedin_uses_last_seen_guard(self):
        self._assert_guard(run_details, source="linkedin")

    def test_stepstone_uses_last_seen_guard(self):
        self._assert_guard(run_details_stepstone)

    def test_xing_uses_last_seen_guard(self):
        self._assert_guard(run_details_xing)

    def test_default_window_is_60_days_in_all_mains(self):
        self.assertIn('DETAIL_LAST_SEEN_WINDOW_DAYS", "60"', inspect.getsource(run_details.main))
        self.assertIn('DETAIL_LAST_SEEN_WINDOW_DAYS", "60"', inspect.getsource(run_details_stepstone.main))
        self.assertIn('DETAIL_LAST_SEEN_WINDOW_DAYS", "60"', inspect.getsource(run_details_xing.main))


if __name__ == "__main__":
    unittest.main()
