import tempfile
import unittest
from pathlib import Path

from job_scrape.xing_config import load_xing_config


class TestXingConfig(unittest.TestCase):
    def test_keywords_string_parses_as_singleton_tuple(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: s1
      keywords: "Data Engineering"
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_xing_config(p)
            s = cfg.searches[0]
            self.assertEqual(s.keywords, ("Data Engineering",))
            self.assertEqual(s.locations, ())
            self.assertEqual(s.city_ids, {})

    def test_keywords_list_and_optional_locations(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: s1
      keywords:
        - "Data Engineer"
        - "Analytics Engineer"
      locations:
        - "Germany"
      city_ids:
        Germany: "123.abc"
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_xing_config(p)
            s = cfg.searches[0]
            self.assertEqual(s.keywords, ("Data Engineer", "Analytics Engineer"))
            self.assertEqual(s.locations, ("Germany",))
            self.assertEqual(s.city_ids, {"Germany": "123.abc"})

    def test_city_ids_requires_locations(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: s1
      keywords: "Data Engineer"
      city_ids:
        Berlin: "2950159.e2912c"
""".lstrip(),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_xing_config(p)

    def test_city_ids_keys_must_exist_in_locations(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: s1
      keywords: "Data Engineer"
      locations:
        - "Germany"
      city_ids:
        Berlin: "2950159.e2912c"
""".lstrip(),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_xing_config(p)


if __name__ == "__main__":
    unittest.main()

