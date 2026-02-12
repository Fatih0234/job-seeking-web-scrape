import pathlib
import unittest

from job_scrape.stepstone_detail import parse_job_detail


class TestStepstoneDetailExtract(unittest.TestCase):
    def test_parse_job_detail_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "stepstone_job_detail_snippet.html"
        ).read_text(encoding="utf-8")
        d = parse_job_detail(fixture)

        self.assertEqual(d["job_title"], "Manager Financial Control and Reconciliation")
        self.assertEqual(d["company_name"], "N26 GmbH")
        self.assertEqual(d["job_location"], "Berlin, München")
        self.assertEqual(d["posted_time_ago"], "vor 1 Tag")
        self.assertIn("About the opportunity", d["job_description"])
        self.assertIn("Build reconciliation rules", d["job_description"])
        self.assertNotIn("Gehalt anzeigen", d["job_description"])

        self.assertEqual(d["criteria"]["contract_type"], "Feste Anstellung")
        self.assertEqual(d["criteria"]["work_type"], "Homeoffice möglich, Vollzeit")

    def test_parse_job_detail_handles_missing_fields(self):
        d = parse_job_detail("<html><body><h1>Only Title</h1></body></html>")
        self.assertEqual(d["job_title"], "Only Title")
        self.assertIsNone(d["company_name"])
        self.assertIsNone(d["job_description"])


if __name__ == "__main__":
    unittest.main()
