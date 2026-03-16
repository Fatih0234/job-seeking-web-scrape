from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts import refresh_dashboard_read_models


class TestRefreshDashboardReadModels(unittest.TestCase):
    def test_refresh_runs_all_steps_in_dependency_order(self):
        calls: list[str] = []

        def mark(name: str):
            def _inner() -> None:
                calls.append(name)

            return _inner

        with (
            patch.object(refresh_dashboard_read_models.create_dashboard_view, "main", side_effect=mark("dashboard_view")),
            patch.object(
                refresh_dashboard_read_models.create_dashboard_map_view,
                "main",
                side_effect=mark("dashboard_map_view"),
            ),
            patch.object(
                refresh_dashboard_read_models.create_dashboard_analytics_views,
                "main",
                side_effect=mark("dashboard_analytics_views"),
            ),
            patch.object(
                refresh_dashboard_read_models.create_dashboard_materialized_views,
                "main",
                side_effect=mark("dashboard_materialized_views"),
            ),
            patch.object(refresh_dashboard_read_models.create_target_job_views, "main", side_effect=mark("target_job_views")),
            patch.object(
                refresh_dashboard_read_models.create_working_student_app_views,
                "main",
                side_effect=mark("working_student_app_views"),
            ),
            patch.object(refresh_dashboard_read_models.create_public_api_views, "main", side_effect=mark("public_api_views")),
        ):
            executed = refresh_dashboard_read_models.refresh_dashboard_read_models()

        self.assertEqual(
            executed,
            [
                "dashboard_view",
                "dashboard_map_view",
                "dashboard_analytics_views",
                "dashboard_materialized_views",
                "target_job_views",
                "working_student_app_views",
                "public_api_views",
            ],
        )
        self.assertEqual(calls, executed)

    def test_custom_steps_are_supported(self):
        calls: list[str] = []

        def first() -> None:
            calls.append("first")

        def second() -> None:
            calls.append("second")

        executed = refresh_dashboard_read_models.refresh_dashboard_read_models(
            [("first", first), ("second", second)]
        )

        self.assertEqual(executed, ["first", "second"])
        self.assertEqual(calls, ["first", "second"])


if __name__ == "__main__":
    unittest.main()
