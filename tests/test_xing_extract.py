import pathlib
import unittest

from job_scrape.xing import (
    build_search_url,
    extract_job_id_from_href,
    has_show_more,
    parse_search_results,
)


class TestXingExtract(unittest.TestCase):
    def test_build_search_url_keywords_only(self):
        url = build_search_url(keywords="Data Engineering")
        self.assertIn("https://www.xing.com/jobs/search/ki?", url)
        self.assertIn("keywords=Data+Engineering", url)
        self.assertNotIn("location=", url)
        self.assertNotIn("cityId=", url)
        self.assertNotIn("id=", url)

    def test_build_search_url_with_location(self):
        url = build_search_url(keywords="Data Engineering", location_text="Germany")
        self.assertIn("keywords=Data+Engineering", url)
        self.assertIn("location=Germany", url)
        self.assertNotIn("cityId=", url)

    def test_build_search_url_with_location_and_city_id(self):
        url = build_search_url(
            keywords="Data Engineering",
            location_text="Berlin",
            city_id="2950159.e2912c",
        )
        self.assertIn("location=Berlin", url)
        self.assertIn("cityId=2950159.e2912c", url)

    def test_extract_job_id_from_href(self):
        self.assertEqual(extract_job_id_from_href("/jobs/berlin-data-engineer-123456789"), "123456789")
        self.assertEqual(
            extract_job_id_from_href("https://www.xing.com/jobs/vienna-analytics-engineer-222333444?trk=foo"),
            "222333444",
        )
        self.assertIsNone(extract_job_id_from_href("https://www.xing.com/jobs/search/ki"))

    def test_parse_search_results_fixture(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "xing_search_snippet.html"
        ).read_text(encoding="utf-8")
        items = parse_search_results(
            fixture,
            search_url="https://www.xing.com/jobs/search/ki?keywords=data+engineering",
        )
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["source"], "xing")
        self.assertEqual(items[0]["job_id"], "123456789")
        self.assertEqual(items[0]["job_url"], "https://www.xing.com/jobs/berlin-data-engineer-123456789")
        self.assertEqual(items[1]["job_id"], "222333444")
        self.assertEqual(items[1]["job_url"], "https://www.xing.com/jobs/vienna-analytics-engineer-222333444")
        self.assertEqual(items[2]["job_id"], "222333444")

    def test_has_show_more(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "xing_search_snippet.html"
        ).read_text(encoding="utf-8")
        self.assertTrue(has_show_more(fixture))
        self.assertFalse(has_show_more("<html><body><button>Reset filters</button></body></html>"))


if __name__ == "__main__":
    unittest.main()

