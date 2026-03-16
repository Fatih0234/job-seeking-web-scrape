from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.xing_cron_diagnostics import collect_run_mismatches, reconcile_gh_runs_to_db


class TestXingCronDiagnostics(unittest.TestCase):
    def test_reconciliation_and_mismatch_classification(self):
        t0 = datetime(2026, 2, 15, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 2, 15, 3, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc)

        gh_rows = [
            {
                "workflow": "XING Crawl (Last 24 Hours)",
                "run_id": 1001,
                "event": "schedule",
                "status": "completed",
                "conclusion": "success",
                "created_at": t0,
                "updated_at": t0,
                "url": "https://example.test/1001",
            },
            {
                "workflow": "XING Crawl (Last 24 Hours)",
                "run_id": 1002,
                "event": "schedule",
                "status": "completed",
                "conclusion": "failure",
                "created_at": t1,
                "updated_at": t1,
                "url": "https://example.test/1002",
            },
        ]

        db_rows = [
            {
                "id": "db-run-failure-match",
                "trigger": "github_schedule_last24h",
                "status": "running",
                "started_at": t1,
                "finished_at": None,
                "error": None,
            },
            {
                "id": "db-run-details-without-gh",
                "trigger": "github_schedule_xing_details",
                "status": "success",
                "started_at": t2,
                "finished_at": t2,
                "error": None,
            },
        ]

        reconciled = reconcile_gh_runs_to_db(
            gh_rows=gh_rows,
            db_rows=db_rows,
            max_delta_seconds=1200,
        )

        mismatches = collect_run_mismatches(
            gh_reconciled=reconciled,
            gh_rows=gh_rows,
            db_rows=db_rows,
            max_delta_seconds=1200,
        )

        codes = {m["code"] for m in mismatches}
        self.assertIn("gh_success_without_db_row", codes)
        self.assertIn("gh_non_success_with_lingering_running_db_row", codes)
        self.assertIn("db_schedule_without_gh_run", codes)


if __name__ == "__main__":
    unittest.main()
