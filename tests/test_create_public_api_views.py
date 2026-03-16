from __future__ import annotations

import unittest

from scripts.create_public_api_views import SQL


class TestCreatePublicApiViews(unittest.TestCase):
    def test_includes_working_student_public_proxies(self):
        self.assertIn("create or replace view public.working_student_jobs_v as", SQL)
        self.assertIn("create or replace view public.working_student_city_bubbles_v as", SQL)
        self.assertIn("create or replace view public.working_student_map_points_v as", SQL)
        self.assertIn("create or replace view public.working_student_kpis_v as", SQL)
        self.assertIn("create or replace view public.working_student_trend_v as", SQL)
        self.assertIn("create or replace view public.working_student_top_skills_v as", SQL)
        self.assertIn("grant select on public.working_student_jobs_v to anon, authenticated;", SQL)


if __name__ == "__main__":
    unittest.main()
