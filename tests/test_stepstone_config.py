import tempfile
import unittest
from pathlib import Path

from job_scrape.stepstone_config import load_stepstone_config


class TestStepstoneConfig(unittest.TestCase):
    def test_load_defaults_and_sort_label(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
stepstone:
  searches:
    - name: s1
      keywords: "Data Engineering"
      locations:
        - "Regensburg"
      sort: newest
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_stepstone_config(p)
            s = cfg.searches[0]
            self.assertEqual(s.sort, 2)
            self.assertEqual(s.radius, 30)
            self.assertEqual(s.where_type, "autosuggest")

    def test_invalid_sort_raises(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
stepstone:
  searches:
    - name: s1
      keywords: "Data Engineering"
      locations: "Berlin"
      sort: "oldest"
""".lstrip(),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_stepstone_config(p)


if __name__ == "__main__":
    unittest.main()
