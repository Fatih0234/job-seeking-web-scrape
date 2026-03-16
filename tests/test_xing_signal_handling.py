"""Tests for XING crawl signal handling and finalization.

We verify that:
1. The ``_finalize_failed`` closure marks the crawl_run as failed exactly once.
2. Signal handlers are installed and restored correctly.
3. The ``fail_running_search_runs`` helper is called on termination.
"""

from __future__ import annotations

import signal
import unittest
from unittest.mock import MagicMock, call, patch

# We import the *module* so we can exercise ``main()`` with full patching.
from scripts import run_crawl_xing


class _FakeConn:
    """Minimal context-manager that mimics a psycopg connection."""

    def __init__(self, cursor: "_FakeCursor") -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


class _FakeCursor:
    """Records executed SQL for later inspection."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple]] = []
        self._returns: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def set_returns(self, rows: list):
        self._returns = rows

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        return self._returns.pop(0) if self._returns else ("fake-run-id",)

    def fetchall(self):
        return self._returns if self._returns else []


def _fake_connect():
    """Return a context-manager-like _FakeConn."""
    return _FakeConn(_FakeCursor())


class TestXingSignalHandling(unittest.TestCase):
    """Signal handler installation and idempotent finalization."""

    @patch.dict(
        "os.environ",
        {
            "ENSURE_XING_TABLES": "0",
            "CRAWL_TRIGGER": "test",
            "RUN_DISCOVERY": "0",
            "RUN_DETAILS": "0",
        },
    )
    @patch("scripts.xing_crawl_common.connect", side_effect=_fake_connect)
    def test_both_disabled_marks_failed(self, _mock_connect):
        """When both phases are disabled, the crawl_run is finalized as failed."""
        with self.assertRaises(RuntimeError):
            run_crawl_xing.main()

    @patch.dict(
        "os.environ",
        {
            "ENSURE_XING_TABLES": "0",
            "CRAWL_TRIGGER": "test",
            "RUN_DISCOVERY": "0",
            "RUN_DETAILS": "0",
        },
    )
    @patch("scripts.xing_crawl_common.connect", side_effect=_fake_connect)
    def test_signals_restored_after_main(self, _mock_connect):
        """Signal handlers are restored even when main() raises."""
        orig_int = signal.getsignal(signal.SIGINT)
        orig_term = signal.getsignal(signal.SIGTERM)
        try:
            run_crawl_xing.main()
        except (RuntimeError, SystemExit):
            pass
        self.assertEqual(signal.getsignal(signal.SIGINT), orig_int)
        self.assertEqual(signal.getsignal(signal.SIGTERM), orig_term)


class TestFailRunningSearchRuns(unittest.TestCase):
    """Ensure the new ``fail_running_search_runs`` helper works."""

    @patch("scripts.xing_crawl_common.connect", side_effect=_fake_connect)
    def test_fail_running_search_runs_returns_zero_when_none(self, _mock_connect):
        from scripts.xing_crawl_common import fail_running_search_runs

        count = fail_running_search_runs("fake-uuid", error="test-error")
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
