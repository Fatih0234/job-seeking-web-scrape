from __future__ import annotations

import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


class _FakeCursor:
    def __init__(self, returns: list[tuple] | None = None) -> None:
        self._returns = list(returns or [])
        self.queries: list[tuple[str, tuple | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        return self._returns.pop(0) if self._returns else None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def cursor(self):
        return self._cursor


class TestXingDetailsCooldown(unittest.TestCase):
    def test_recent_blocked_run_within_true(self):
        from scripts.run_details_xing import _recent_blocked_run_within

        finished_at = datetime.now(timezone.utc)
        cursor = _FakeCursor(returns=[(finished_at,), (True,)])
        conn = _FakeConn(cursor)
        with patch("scripts.run_details_xing.connect", return_value=conn):
            self.assertTrue(_recent_blocked_run_within(cooldown_minutes=180))

    def test_main_skips_when_cooldown_active(self):
        from scripts import run_details_xing

        finished_at = datetime.now(timezone.utc)
        cursor = _FakeCursor(returns=[(finished_at,), (True,)])
        conn = _FakeConn(cursor)

        buf = io.StringIO()
        with (
            patch.dict(
                "os.environ",
                {
                    "CRAWL_RUN_ID": "fake-run-id",
                    "DETAIL_COOLDOWN_AFTER_BLOCK_MINUTES": "180",
                    "MAX_JOB_DETAILS_PER_RUN": "10",
                    "DETAIL_STALENESS_DAYS": "7",
                    "DETAIL_BLOCKED_RETRY_HOURS": "24",
                    "DETAIL_LAST_SEEN_WINDOW_DAYS": "7",
                },
            ),
            patch("scripts.run_details_xing.connect", return_value=conn),
            patch("sys.stdout", new=buf),
        ):
            run_details_xing.main()

        payload = json.loads(buf.getvalue().strip())
        self.assertEqual(payload.get("status"), "skipped_backoff")
        self.assertEqual(payload.get("crawl_run_id"), "fake-run-id")
        self.assertEqual(payload.get("counts", {}).get("detail_jobs_selected"), 0)


if __name__ == "__main__":
    unittest.main()
