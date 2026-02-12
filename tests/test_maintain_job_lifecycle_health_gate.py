import unittest
from datetime import datetime, timedelta, timezone

from scripts import maintain_job_lifecycle


class TestLifecycleHealthGate(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 2, 12, 12, 0, tzinfo=timezone.utc)

    def test_skips_when_no_recent_run(self):
        action, note = maintain_job_lifecycle._decide_platform_action(
            latest_run=None,
            now_utc=self.now,
            max_crawl_age_hours=36,
        )
        self.assertEqual(action, "skipped_no_recent_run")
        self.assertIn("No crawl_run rows", note)

    def test_skips_when_latest_status_not_success(self):
        action, note = maintain_job_lifecycle._decide_platform_action(
            latest_run=("run-1", "blocked", self.now - timedelta(hours=2)),
            now_utc=self.now,
            max_crawl_age_hours=36,
        )
        self.assertEqual(action, "skipped_unhealthy")
        self.assertIn("expected 'success'", note)

    def test_skips_when_latest_success_is_too_old(self):
        action, note = maintain_job_lifecycle._decide_platform_action(
            latest_run=("run-2", "success", self.now - timedelta(hours=48)),
            now_utc=self.now,
            max_crawl_age_hours=36,
        )
        self.assertEqual(action, "skipped_unhealthy")
        self.assertIn("too old", note)

    def test_processes_when_latest_success_is_recent(self):
        action, note = maintain_job_lifecycle._decide_platform_action(
            latest_run=("run-3", "success", self.now - timedelta(hours=1)),
            now_utc=self.now,
            max_crawl_age_hours=36,
        )
        self.assertEqual(action, "processed")
        self.assertIsNone(note)


if __name__ == "__main__":
    unittest.main()
