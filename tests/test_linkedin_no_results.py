import pathlib
import unittest

from job_scrape.linkedin import parse_no_results_box, parse_search_results


class TestLinkedInNoResults(unittest.TestCase):
    def test_parse_no_results_box_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "linkedin_no_results_snippet.html"
        ).read_text(encoding="utf-8")

        self.assertEqual(parse_search_results(fixture, search_url="https://www.linkedin.com/jobs/search?x=1"), [])

        meta = parse_no_results_box(fixture)
        self.assertIsNotNone(meta)
        assert meta is not None
        self.assertEqual(meta.get("keywords"), "Data Engineering jobs in Germany")
        self.assertEqual(meta.get("subheading"), "Please make sure your keywords are spelled correctly")
        self.assertIn("We couldn't find a match", meta.get("title_text") or "")

    def test_parse_no_results_box_absent_on_normal_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "linkedin_search_snippet.html"
        ).read_text(encoding="utf-8")
        self.assertIsNone(parse_no_results_box(fixture))


if __name__ == "__main__":
    unittest.main()

