import tempfile
import unittest
from pathlib import Path

from job_scrape.skill_extraction import extract_grouped_skills, load_skill_taxonomy


class TestSkillExtraction(unittest.TestCase):
    def test_extract_grouped_skills_basic(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tax.yaml"
            p.write_text(
                """
version: 1
groups:
  languages:
    - canonical: Python
      aliases: ["python", "pandas"]
    - canonical: SQL
      aliases: ["sql"]
  cloud_platforms:
    - canonical: AWS
      aliases: ["aws", "s3"]
""".lstrip(),
                encoding="utf-8",
            )
            tax = load_skill_taxonomy(p)
            text = "We use Python, pandas, and SQL. Experience with AWS (S3) preferred."
            out = extract_grouped_skills(text, taxonomy=tax)
            self.assertEqual(out["languages"], ["Python", "SQL"])
            self.assertEqual(out["cloud_platforms"], ["AWS"])

    def test_short_alpha_alias_is_ignored(self):
        # Avoid false positives like "go" in normal sentences.
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "tax.yaml"
            p.write_text(
                """
version: 1
groups:
  languages:
    - canonical: Go
      aliases: ["go", "golang"]
""".lstrip(),
                encoding="utf-8",
            )
            tax = load_skill_taxonomy(p)
            self.assertEqual(extract_grouped_skills("Please go to the office.", taxonomy=tax), {})
            self.assertEqual(extract_grouped_skills("We use Golang in production.", taxonomy=tax), {"languages": ["Go"]})


if __name__ == "__main__":
    unittest.main()

