from __future__ import annotations

import unittest

from scripts.verify_xing_workflow_run import evaluate_integrity, repair_stale_running_runs


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple | None]] = []
        self.last_sql = ""

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self.last_sql = sql

    def fetchall(self):
        if "update job_scrape.xing_search_runs" in self.last_sql:
            return [("s1",), ("s2",)]
        if "update job_scrape.xing_crawl_runs" in self.last_sql:
            return [("c1",)]
        return []


class TestVerifyXingWorkflowRun(unittest.TestCase):
    def test_repair_stale_running_runs_updates_search_and_crawl_rows(self):
        cur = _FakeCursor()
        out = repair_stale_running_runs(
            cur,
            run_ids=["id-1", "id-2"],
            reason="unit-test-repair",
        )
        self.assertEqual(out["repaired_search_runs"], 4)
        self.assertEqual(out["repaired_crawl_runs"], 2)
        self.assertEqual(len(cur.calls), 4)

    def test_success_without_crawl_run_id_is_violation(self):
        _checks, violations = evaluate_integrity(
            expected_trigger="github_schedule_last24h",
            run_step_outcome="success",
            crawl_run_id=None,
            crawl_row=None,
            stale_after_ids=[],
            lingering_running_rows=[],
        )
        codes = {v["code"] for v in violations}
        self.assertIn("missing_crawl_run_id", codes)
        self.assertIn("missing_crawl_row", codes)

    def test_success_with_running_db_status_is_violation(self):
        _checks, violations = evaluate_integrity(
            expected_trigger="github_schedule_last24h",
            run_step_outcome="success",
            crawl_run_id="run-1",
            crawl_row={
                "id": "run-1",
                "trigger": "github_schedule_last24h",
                "status": "running",
            },
            stale_after_ids=[],
            lingering_running_rows=[],
        )
        codes = {v["code"] for v in violations}
        self.assertIn("non_terminal_crawl_row", codes)

    def test_non_success_with_lingering_running_rows_is_violation(self):
        _checks, violations = evaluate_integrity(
            expected_trigger="github_schedule_last24h",
            run_step_outcome="cancelled",
            crawl_run_id="",
            crawl_row=None,
            stale_after_ids=[],
            lingering_running_rows=[{"id": "run-1", "status": "running"}],
        )
        codes = {v["code"] for v in violations}
        self.assertIn("lingering_running_after_non_success", codes)

    def test_stale_rows_remain_is_violation(self):
        _checks, violations = evaluate_integrity(
            expected_trigger="github_schedule_last24h",
            run_step_outcome="success",
            crawl_run_id="run-1",
            crawl_row={
                "id": "run-1",
                "trigger": "github_schedule_last24h",
                "status": "success",
            },
            stale_after_ids=["old-run"],
            lingering_running_rows=[],
        )
        codes = {v["code"] for v in violations}
        self.assertIn("stale_rows_remain", codes)


if __name__ == "__main__":
    unittest.main()
