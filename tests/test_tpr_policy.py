import unittest
from datetime import datetime, timezone

from job_scrape.tpr_policy import apply_auto_tpr_if_any_time, normalize_facets


class TestTPRPolicy(unittest.TestCase):
    def test_normalize_drops_none_and_empty_tpr(self):
        self.assertEqual(
            normalize_facets({"f_TPR": "", "f_JT": ["F"], "x": None}),
            {"f_JT": ["F"]},
        )

    def test_first_run_no_history_keeps_any_time(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        out = apply_auto_tpr_if_any_time(
            facets={"f_TPR": ""},  # stored Any time
            has_finished_history=False,
            last_success_finished_at=None,
            now_utc=now,
        )
        self.assertNotIn("f_TPR", out)

    def test_recent_success_uses_past_24h(self):
        now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
        last = datetime(2026, 1, 2, 0, 30, tzinfo=timezone.utc)  # 11.5h ago
        out = apply_auto_tpr_if_any_time(
            facets={},  # Any time
            has_finished_history=True,
            last_success_finished_at=last,
            now_utc=now,
            recent_hours=30,
            recent_code="r86400",
            fallback_code="r604800",
        )
        self.assertEqual(out.get("f_TPR"), "r86400")

    def test_stale_success_uses_past_week(self):
        now = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)
        last = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)  # 60h ago
        out = apply_auto_tpr_if_any_time(
            facets={},
            has_finished_history=True,
            last_success_finished_at=last,
            now_utc=now,
            recent_hours=30,
            recent_code="r86400",
            fallback_code="r604800",
        )
        self.assertEqual(out.get("f_TPR"), "r604800")

    def test_explicit_tpr_is_not_overridden(self):
        now = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)
        out = apply_auto_tpr_if_any_time(
            facets={"f_TPR": "r2592000"},
            has_finished_history=True,
            last_success_finished_at=None,
            now_utc=now,
        )
        self.assertEqual(out.get("f_TPR"), "r2592000")


if __name__ == "__main__":
    unittest.main()

