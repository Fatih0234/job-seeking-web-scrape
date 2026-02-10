import tempfile
import unittest
from pathlib import Path

from job_scrape.yaml_config import load_linkedin_config


class TestYamlConfigKeywordsList(unittest.TestCase):
    def test_keywords_string_parses_as_singleton_tuple(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
linkedin:
  searches:
    - name: s1
      keywords: "Data Engineering"
      countries:
        - name: Germany
      filters: {}
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_linkedin_config(p)
            self.assertEqual(cfg.searches[0].keywords, ("Data Engineering",))

    def test_keywords_list_parses_as_tuple(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
linkedin:
  searches:
    - name: s1
      keywords:
        - "Data Engineering"
        - "Analytics Engineer"
      countries:
        - name: Germany
      filters: {}
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_linkedin_config(p)
            self.assertEqual(cfg.searches[0].keywords, ("Data Engineering", "Analytics Engineer"))

    def test_keywords_invalid_type_raises(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
linkedin:
  searches:
    - name: s1
      keywords:
        x: y
      countries:
        - name: Germany
      filters: {}
""".lstrip(),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_linkedin_config(p)


if __name__ == "__main__":
    unittest.main()

