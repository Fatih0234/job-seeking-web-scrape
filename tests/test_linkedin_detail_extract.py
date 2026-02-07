import pathlib
import unittest

from job_scrape.linkedin_detail import parse_job_detail


class TestLinkedInDetailExtract(unittest.TestCase):
    def test_parse_job_detail_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "linkedin_job_detail_snippet.html"
        ).read_text(encoding="utf-8")
        d = parse_job_detail(fixture)
        self.assertEqual(d["job_title"], "Chief of Staff to the CTO")
        self.assertEqual(d["company_name"], "Cint")
        self.assertEqual(d["job_location"], "Berlin, Berlin, Germany")
        self.assertEqual(d["posted_time_ago"], "2 hours ago")
        self.assertIn("Cint is a pioneer in research technology", d["job_description"])
        self.assertIn("support our CTO based in Berlin", d["job_description"])
        self.assertEqual(d["criteria"]["seniority_level"], "Mid-Senior level")
        self.assertEqual(d["criteria"]["employment_type"], "Full-time")
        self.assertEqual(d["criteria"]["job_function"], "Administrative")
        self.assertEqual(d["criteria"]["industries"], "Software Development")


if __name__ == "__main__":
    unittest.main()
