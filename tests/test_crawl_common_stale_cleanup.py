from __future__ import annotations

import unittest
from unittest.mock import patch


class _FakeCursor:
    def __init__(self, *, fetchall_batches: list[list[tuple]] | None = None) -> None:
        self.fetchall_batches = list(fetchall_batches or [])
        self.queries: list[tuple[str, tuple | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchall(self):
        if not self.fetchall_batches:
            return []
        return self.fetchall_batches.pop(0)


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


class TestCrawlCommonStaleCleanup(unittest.TestCase):
    def test_cleanup_stale_running_crawl_runs_minutes_le_zero_short_circuit(self):
        from scripts.crawl_common import cleanup_stale_running_crawl_runs

        with patch("scripts.crawl_common.connect") as mock_connect:
            out = cleanup_stale_running_crawl_runs(stale_minutes=0)
        self.assertEqual(out, [])
        mock_connect.assert_not_called()

    def test_cleanup_stale_running_crawl_runs_updates_runs_and_searches(self):
        from scripts.crawl_common import cleanup_stale_running_crawl_runs

        cursor = _FakeCursor(fetchall_batches=[[("id1",), ("id2",)]])
        conn = _FakeConn(cursor)

        with patch("scripts.crawl_common.connect", return_value=conn):
            out = cleanup_stale_running_crawl_runs(
                stale_minutes=10,
                status="abandoned",
                error="stale watchdog cleanup",
            )

        self.assertEqual(out, ["id1", "id2"])
        q = [sql for (sql, _params) in cursor.queries]
        self.assertEqual(sum("update job_scrape.search_runs" in s for s in q), 2)
        self.assertEqual(sum("update job_scrape.crawl_runs" in s for s in q), 2)

    def test_fail_running_search_runs_returns_updated_count(self):
        from scripts.crawl_common import fail_running_search_runs

        cursor = _FakeCursor(fetchall_batches=[[(1,), (1,), (1,)]])
        conn = _FakeConn(cursor)

        with patch("scripts.crawl_common.connect", return_value=conn):
            n = fail_running_search_runs("run-123", error="boom")
        self.assertEqual(n, 3)


if __name__ == "__main__":
    unittest.main()
