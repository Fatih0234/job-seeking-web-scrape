import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from scripts import maintain_job_lifecycle


class _UnusedCursor:
    pass


class TestLifecycleCounts(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 2, 12, 12, 0, tzinfo=timezone.utc)
        self.cfg = maintain_job_lifecycle.PLATFORMS[0]

    def test_dry_run_uses_count_paths(self):
        with (
            patch.object(
                maintain_job_lifecycle,
                "_latest_crawl_run",
                return_value=("run-1", "success", self.now - timedelta(hours=2)),
            ),
            patch.object(maintain_job_lifecycle, "_count_soft_expire_candidates", return_value=4),
            patch.object(maintain_job_lifecycle, "_count_hard_delete_candidates", return_value=2),
            patch.object(maintain_job_lifecycle, "_count_hits_for_hard_delete_candidates", return_value=7),
            patch.object(maintain_job_lifecycle, "_count_details_for_hard_delete_candidates", return_value=3),
            patch.object(maintain_job_lifecycle, "_apply_soft_expire") as apply_soft,
            patch.object(maintain_job_lifecycle, "_delete_hits_for_hard_delete_candidates") as delete_hits,
            patch.object(maintain_job_lifecycle, "_delete_details_for_hard_delete_candidates") as delete_details,
            patch.object(maintain_job_lifecycle, "_delete_jobs_for_hard_delete_candidates") as delete_jobs,
        ):
            out = maintain_job_lifecycle._process_platform(
                cur=_UnusedCursor(),
                cfg=self.cfg,
                now_utc=self.now,
                max_crawl_age_hours=36,
                stale_after_days=60,
                hard_delete_after_days=120,
                dry_run=True,
            )

        self.assertEqual(out["action_status"], "processed")
        self.assertEqual(out["stale_marked_count"], 4)
        self.assertEqual(out["hard_delete_candidate_count"], 2)
        self.assertEqual(out["deleted_hits_count"], 7)
        self.assertEqual(out["deleted_details_count"], 3)
        self.assertEqual(out["deleted_jobs_count"], 2)
        apply_soft.assert_not_called()
        delete_hits.assert_not_called()
        delete_details.assert_not_called()
        delete_jobs.assert_not_called()

    def test_live_run_uses_mutation_paths(self):
        with (
            patch.object(
                maintain_job_lifecycle,
                "_latest_crawl_run",
                return_value=("run-2", "success", self.now - timedelta(hours=1)),
            ),
            patch.object(maintain_job_lifecycle, "_apply_soft_expire", return_value=5),
            patch.object(maintain_job_lifecycle, "_count_hard_delete_candidates", return_value=2),
            patch.object(maintain_job_lifecycle, "_delete_hits_for_hard_delete_candidates", return_value=8),
            patch.object(maintain_job_lifecycle, "_delete_details_for_hard_delete_candidates", return_value=3),
            patch.object(maintain_job_lifecycle, "_delete_jobs_for_hard_delete_candidates", return_value=2),
            patch.object(maintain_job_lifecycle, "_count_soft_expire_candidates") as count_soft,
            patch.object(maintain_job_lifecycle, "_count_hits_for_hard_delete_candidates") as count_hits,
            patch.object(maintain_job_lifecycle, "_count_details_for_hard_delete_candidates") as count_details,
        ):
            out = maintain_job_lifecycle._process_platform(
                cur=_UnusedCursor(),
                cfg=self.cfg,
                now_utc=self.now,
                max_crawl_age_hours=36,
                stale_after_days=60,
                hard_delete_after_days=120,
                dry_run=False,
            )

        self.assertEqual(out["action_status"], "processed")
        self.assertEqual(out["stale_marked_count"], 5)
        self.assertEqual(out["hard_delete_candidate_count"], 2)
        self.assertEqual(out["deleted_hits_count"], 8)
        self.assertEqual(out["deleted_details_count"], 3)
        self.assertEqual(out["deleted_jobs_count"], 2)
        count_soft.assert_not_called()
        count_hits.assert_not_called()
        count_details.assert_not_called()


if __name__ == "__main__":
    unittest.main()
