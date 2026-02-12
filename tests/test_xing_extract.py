import pathlib
import unittest

from job_scrape.xing import (
    build_search_url,
    build_external_job_id,
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

    def test_build_search_url_with_since_period(self):
        url = build_search_url(keywords="Data Engineering", since_period="LAST_24_HOURS")
        self.assertIn("keywords=Data+Engineering", url)
        self.assertIn("sincePeriod=LAST_24_HOURS", url)

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
            extract_job_id_from_href(
                "https://www.xing.com/jobs/ulm-erp-systementwickler-mit-schwerpunkt-technische-anwendungen-j32538-150143308"
            ),
            "150143308",
        )
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

    def test_parse_external_job_ad_from_search_card(self):
        html = """
<article data-testid="job-search-result" aria-label="Data Engineer / Ingenieur. Click to open the external job ad in a new tab">
  <a href="https://click.appcast.io/t/abcDEF123?utm_source=xing.com" target="_blank"></a>
  <h2 data-testid="job-teaser-list-title">Data Engineer / Ingenieur</h2>
  <p data-testid="job-teaser-card-company">LHH</p>
  <div class="multi-location-display-styles__Container-sc-cd6c43d-0"><p>Lausanne</p></div>
  <div class="job-teaser-facts__MarkerContainer-sc-c00eb81f-0">
    <span role="status"><span>Full-time</span></span>
    <span role="status"><span>CHF 92,000 – CHF 113,500</span></span>
    <span role="status">
      <span>
        <span aria-hidden="true">External job ad</span>
        <span>External job ad. Posted by a partner.</span>
      </span>
    </span>
  </div>
  <p><time datetime="2026-02-10T12:15:36Z"><span aria-hidden="true">Yesterday</span></time></p>
</article>
""".strip()
        items = parse_search_results(html, search_url="https://www.xing.com/jobs/search/ki?keywords=data+engineer")
        self.assertEqual(len(items), 1)
        it = items[0]
        self.assertTrue(it["is_external"])
        self.assertEqual(it["job_url"], "https://click.appcast.io/t/abcDEF123")
        self.assertEqual(it["job_id"], build_external_job_id("https://click.appcast.io/t/abcDEF123"))
        self.assertEqual(it["list_preview"]["job_title"], "Data Engineer / Ingenieur")
        self.assertEqual(it["list_preview"]["company_name"], "LHH")
        self.assertEqual(it["list_preview"]["job_location"], "Lausanne")
        self.assertEqual(it["list_preview"]["posted_at_utc"], "2026-02-10T12:15:36Z")
        self.assertEqual(it["list_preview"]["employment_type"], "Full-time")
        self.assertEqual(it["list_preview"]["salary_range_text"], "CHF 92,000 – CHF 113,500")


if __name__ == "__main__":
    unittest.main()
