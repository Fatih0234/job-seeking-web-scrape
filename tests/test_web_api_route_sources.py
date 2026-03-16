from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8")


class TestWebApiRouteSources(unittest.TestCase):
    def test_jobs_detail_route_uses_working_student_view(self):
        src = _read("web/src/app/api/jobs/[platform]/[job_id]/route.ts")
        self.assertIn('.from("working_student_jobs_v")', src)
        self.assertNotIn('.from("jobs_dashboard_v")', src)

    def test_analytics_routes_use_working_student_views(self):
        expected = {
            "web/src/app/api/analytics/kpis/route.ts": '.from("working_student_kpis_v")',
            "web/src/app/api/analytics/trend/route.ts": '.from("working_student_trend_v")',
            "web/src/app/api/analytics/top-skills/route.ts": '.from("working_student_top_skills_v")',
            "web/src/app/api/analytics/hotspots/route.ts": '.from("working_student_city_bubbles_v")',
        }
        for path, needle in expected.items():
            with self.subTest(path=path):
                src = _read(path)
                self.assertIn(needle, src)

    def test_map_routes_use_working_student_views(self):
        expected = {
            "web/src/app/api/map/city-jobs/route.ts": '.from("working_student_map_points_v")',
            "web/src/app/api/map/remote/route.ts": '.from("working_student_map_points_v")',
            "web/src/app/api/map/bubbles/route.ts": '.from("working_student_city_bubbles_v")',
        }
        for path, needle in expected.items():
            with self.subTest(path=path):
                src = _read(path)
                self.assertIn(needle, src)

        bubble_src = _read("web/src/app/api/map/bubbles/route.ts")
        self.assertIn('.from("working_student_map_points_v")', bubble_src)
        self.assertNotIn('.from("jobs_dashboard_map_points_v")', bubble_src)
        self.assertNotIn('.from("jobs_dashboard_city_bubbles_v")', bubble_src)


if __name__ == "__main__":
    unittest.main()
