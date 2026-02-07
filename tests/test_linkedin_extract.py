import pathlib
import unittest

from job_scrape.linkedin import canonicalize_job_url, extract_job_id, parse_search_results


class TestLinkedInExtract(unittest.TestCase):
    def test_extract_job_id_from_view_url(self):
        href = "https://www.linkedin.com/jobs/view/4337994473/?trk=public_jobs_topcard-title"
        self.assertEqual(extract_job_id(href), "4337994473")

    def test_extract_job_id_from_view_slug_url(self):
        href = "https://de.linkedin.com/jobs/view/some-role-title-4064488192?position=1&pageNum=0"
        self.assertEqual(extract_job_id(href), "4064488192")

    def test_extract_job_id_from_current_job_id(self):
        href = "https://www.linkedin.com/jobs/search?currentJobId=4064488192&position=1&pageNum=0"
        self.assertEqual(extract_job_id(href), "4064488192")

    def test_extract_job_id_from_urn(self):
        self.assertEqual(extract_job_id(None, entity_urn="urn:li:jobPosting:123456"), "123456")

    def test_extract_job_id_none(self):
        self.assertIsNone(extract_job_id(None))

    def test_canonicalize_job_url_strips_query(self):
        href = "https://www.linkedin.com/jobs/view/4337994473/?trk=public_jobs_topcard-title#foo"
        self.assertEqual(
            canonicalize_job_url(href),
            "https://www.linkedin.com/jobs/view/4337994473/",
        )

    def test_parse_search_results_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "linkedin_search_snippet.html"
        ).read_text(encoding="utf-8")
        items = parse_search_results(fixture, search_url="https://www.linkedin.com/jobs/search?x=1")
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["job_id"], "4337994473")
        self.assertEqual(items[0]["title"], "Data Engineer")
        self.assertEqual(items[0]["company"], "ACME Corp")
        self.assertEqual(items[1]["job_id"], "4064488192")
        self.assertEqual(items[1]["company"], "Beta GmbH")


if __name__ == "__main__":
    unittest.main()
