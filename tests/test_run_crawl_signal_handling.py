from __future__ import annotations

import signal
import unittest
from unittest.mock import patch

from scripts import run_crawl


class TestRunCrawlStatus(unittest.TestCase):
    def test_derive_crawl_status(self):
        self.assertEqual(
            run_crawl._derive_crawl_status(discovery_status="success", details_status="success"),
            "success",
        )
        self.assertEqual(
            run_crawl._derive_crawl_status(discovery_status="blocked", details_status="success"),
            "blocked",
        )
        self.assertEqual(
            run_crawl._derive_crawl_status(discovery_status="blocked", details_status="failed"),
            "failed",
        )


class TestRunCrawlSignalHandling(unittest.TestCase):
    @patch.dict(
        "os.environ",
        {
            "CRAWL_TRIGGER": "test_trigger",
            "RUN_DISCOVERY": "1",
            "RUN_DETAILS": "0",
            "SYNC_SEARCH_DEFINITIONS": "0",
        },
    )
    @patch("scripts.run_crawl.cleanup_stale_running_crawl_runs", return_value=[])
    @patch("scripts.run_crawl.create_crawl_run", return_value="run-1")
    @patch("scripts.run_crawl.load_enabled_searches", return_value=[{"search_definition_id": "sid-1"}])
    @patch("scripts.run_crawl.create_search_runs")
    @patch("scripts.run_crawl.fail_running_search_runs")
    @patch("scripts.run_crawl.finish_crawl_run")
    def test_sigterm_finalizes_run(
        self,
        mock_finish,
        mock_fail_running,
        _mock_create_search_runs,
        _mock_load_enabled,
        _mock_create,
        _mock_cleanup,
    ):
        handlers: dict[int, object] = {}

        def fake_signal(sig, handler):
            handlers[sig] = handler
            return None

        def fake_run(*_args, **_kwargs):
            term_handler = handlers[signal.SIGTERM]
            term_handler(signal.SIGTERM, None)
            return ""

        with (
            patch("scripts.run_crawl.signal.getsignal", return_value=signal.SIG_DFL),
            patch("scripts.run_crawl.signal.signal", side_effect=fake_signal),
            patch("scripts.run_crawl._run", side_effect=fake_run),
            self.assertRaises(SystemExit),
        ):
            run_crawl.main()

        mock_fail_running.assert_called_once_with("run-1", error="terminated_by_signal_15")
        mock_finish.assert_called_once_with("run-1", status="failed", stats={}, error="terminated_by_signal_15")

    @patch.dict(
        "os.environ",
        {
            "CRAWL_TRIGGER": "test_trigger",
            "RUN_DISCOVERY": "0",
            "RUN_DETAILS": "0",
        },
    )
    @patch("scripts.run_crawl.cleanup_stale_running_crawl_runs", return_value=[])
    @patch("scripts.run_crawl.create_crawl_run", return_value="run-2")
    @patch("scripts.run_crawl.fail_running_search_runs")
    @patch("scripts.run_crawl.finish_crawl_run")
    @patch("scripts.run_crawl._run")
    def test_both_disabled_marks_failed(
        self,
        mock_run,
        mock_finish,
        mock_fail_running,
        _mock_create,
        _mock_cleanup,
    ):
        with (
            patch("scripts.run_crawl.signal.getsignal", return_value=signal.SIG_DFL),
            patch("scripts.run_crawl.signal.signal"),
            self.assertRaises(RuntimeError),
        ):
            run_crawl.main()

        mock_run.assert_not_called()
        mock_fail_running.assert_called_once()
        args, kwargs = mock_finish.call_args
        self.assertEqual(args[0], "run-2")
        self.assertEqual(kwargs["status"], "failed")
        self.assertIn("Nothing to do", kwargs["error"])


if __name__ == "__main__":
    unittest.main()
