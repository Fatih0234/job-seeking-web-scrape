import unittest

from job_scrape.xing_detail import parse_job_detail


class TestXingDetailExtract(unittest.TestCase):
    def test_parse_job_detail_prefers_jsonld_for_exact_posted_datetime(self):
        html = """
<html><body>
  <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "JobPosting",
      "title": "Senior Data Engineer (m/w/d)",
      "datePosted": "2026-02-06T10:39:42Z",
      "employmentType": "Full-time",
      "description": "<p>Build data pipelines.</p><p>Own platform quality.</p>",
      "hiringOrganization": {"@type": "Organization", "name": "Acme GmbH"},
      "jobLocation": {
        "@type": "Place",
        "address": {
          "@type": "PostalAddress",
          "addressLocality": "Hamburg",
          "addressRegion": "HH",
          "addressCountry": "DE"
        }
      }
    }
  </script>
  <h1>Fallback title should not win</h1>
  <ul aria-label="Main details for this job:">
    <li role="status"><span>Full-time</span></li>
    <li role="status"><span>€64,000 – €78,500 (XING estimate)</span></li>
    <li role="status"><span>Hybrid</span></li>
  </ul>
  <p data-testid="job-details-published-date">
    <time datetime="2026-02-05T01:00:00Z"><span aria-hidden="true">5 days ago</span></time>
  </p>
</body></html>
""".strip()
        d = parse_job_detail(html)
        self.assertEqual(d["job_title"], "Senior Data Engineer (m/w/d)")
        self.assertEqual(d["company_name"], "Acme GmbH")
        self.assertEqual(d["job_location"], "Hamburg, HH, DE")
        self.assertEqual(d["posted_at_utc"], "2026-02-06T10:39:42+00:00")
        self.assertEqual(d["employment_type"], "Full-time")
        self.assertEqual(d["salary_range_text"], "€64,000 – €78,500 (XING estimate)")
        self.assertEqual(d["work_model"], "Hybrid")
        self.assertIn("Build data pipelines.", d["job_description"])

    def test_parse_job_detail_falls_back_to_dom_when_jsonld_missing(self):
        html = """
<html><body>
  <h1>Data Engineer</h1>
  <p data-testid="job-details-company-info-name">Data Co</p>
  <div class="multi-location-display-styles__Container-sc-cd6c43d-0"><p>Berlin</p></div>
  <p data-testid="job-details-published-date">
    <time datetime="2026-02-01T08:30:00Z"><span aria-hidden="true">10 days ago</span></time>
  </p>
  <ul aria-label="Main details for this job:">
    <li role="status"><span>Self-employed</span></li>
    <li role="status"><span>Remote</span></li>
  </ul>
</body></html>
""".strip()
        d = parse_job_detail(html)
        self.assertEqual(d["job_title"], "Data Engineer")
        self.assertEqual(d["company_name"], "Data Co")
        self.assertEqual(d["job_location"], "Berlin")
        self.assertEqual(d["posted_at_utc"], "2026-02-01T08:30:00+00:00")
        self.assertEqual(d["posted_time_ago"], "10 days ago")
        self.assertEqual(d["employment_type"], "Self-employed")
        self.assertEqual(d["work_model"], "Remote")
        self.assertIsNone(d["salary_range_text"])


if __name__ == "__main__":
    unittest.main()
