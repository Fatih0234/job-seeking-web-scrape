import pathlib
import unittest

from job_scrape.stepstone import (
    build_search_url,
    extract_job_id,
    normalize_sort,
    parse_result_counters,
    parse_search_results,
    parse_section_markers,
    select_main_results,
)


class TestStepstoneExtract(unittest.TestCase):
    def test_build_search_url_defaults(self):
        url = build_search_url(
            keywords="Data Platform Engineering",
            location="26121 Oldenburg",
        )
        self.assertTrue(url.startswith("https://www.stepstone.de/jobs/data-platform-engineering/in-26121-oldenburg?"))
        self.assertIn("radius=30", url)
        self.assertIn("sort=2", url)
        self.assertIn("whereType=autosuggest", url)
        self.assertIn("searchOrigin=Resultlist_top-search", url)
        self.assertIn("action=sort_publish", url)
        self.assertNotIn("page=", url)

    def test_build_search_url_page_2_uses_paging_action(self):
        url = build_search_url(
            keywords="Data Engineering",
            location="Regensburg",
            sort=1,
            page=2,
        )
        self.assertIn("page=2", url)
        self.assertIn("sort=1", url)
        self.assertIn("action=paging_next", url)

    def test_normalize_sort_accepts_labels_and_numbers(self):
        self.assertEqual(normalize_sort("relevance"), 1)
        self.assertEqual(normalize_sort("newest"), 2)
        self.assertEqual(normalize_sort("2"), 2)

    def test_extract_job_id(self):
        self.assertEqual(extract_job_id("job-item-13517844"), "13517844")
        self.assertIsNone(extract_job_id("job-item-abc"))

    def test_parse_search_results_and_main_slice(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "stepstone_search_snippet.html"
        ).read_text(encoding="utf-8")

        counters = parse_result_counters(fixture)
        self.assertIsNotNone(counters)
        assert counters is not None
        self.assertEqual(counters.main, 37)
        self.assertEqual(counters.main_displayed, 2)
        self.assertEqual(counters.semantic_displayed, 23)

        items = parse_search_results(
            fixture,
            search_url="https://www.stepstone.de/jobs/data-engineering/in-berlin?radius=30",
        )
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["job_id"], "13517844")
        self.assertEqual(items[0]["job_url"], "https://www.stepstone.de/job/13517844")

        main = select_main_results(items, counters=counters)
        self.assertEqual(len(main), 2)
        self.assertEqual([x["job_id"] for x in main], ["13517844", "13641451"])

    def test_parse_section_markers(self):
        fixture = (
            pathlib.Path(__file__).parent / "fixtures" / "stepstone_search_snippet.html"
        ).read_text(encoding="utf-8")
        markers = parse_section_markers(fixture)
        self.assertEqual(len(markers), 1)
        self.assertIn("Noch nichts dabei?", markers[0])


if __name__ == "__main__":
    unittest.main()
