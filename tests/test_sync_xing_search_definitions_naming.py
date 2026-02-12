import tempfile
import unittest
from pathlib import Path

from job_scrape.xing_config import load_xing_config
from scripts.sync_search_definitions_xing import build_search_definition_name, iter_search_definition_rows


class TestSyncXingSearchDefinitionsNaming(unittest.TestCase):
    def test_name_is_stable_for_no_location(self):
        name = build_search_definition_name(
            base="xing_data_eng",
            location=None,
            location_idx=0,
            kw_idx=1,
            keyword="Data Platform Engineer",
        )
        self.assertEqual(name, "xing_data_eng__locall__kw1_data_platform_engineer")

    def test_name_is_stable_for_location(self):
        name = build_search_definition_name(
            base="xing_data_eng",
            location="Germany",
            location_idx=2,
            kw_idx=0,
            keyword="Data Engineer",
        )
        self.assertEqual(name, "xing_data_eng__loc2_germany__kw0_data_engineer")

    def test_name_changes_by_location_and_keyword_idx(self):
        a = build_search_definition_name(
            base="s",
            location="Germany",
            location_idx=0,
            kw_idx=0,
            keyword="Data Engineer",
        )
        b = build_search_definition_name(
            base="s",
            location="Austria",
            location_idx=1,
            kw_idx=0,
            keyword="Data Engineer",
        )
        c = build_search_definition_name(
            base="s",
            location="Germany",
            location_idx=0,
            kw_idx=1,
            keyword="Data Engineer",
        )
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, c)

    def test_iter_rows_has_stable_keyword_indexes_after_dedup(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: xing_data_eng
      keywords:
        - "Data Engineer"
        - "data engineer"
        - "SQL Developer"
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_xing_config(p)
            rows = iter_search_definition_rows(cfg)
            names = [row["name"] for row in rows]
            self.assertEqual(
                names,
                [
                    "xing_data_eng__locall__kw0_data_engineer",
                    "xing_data_eng__locall__kw1_sql_developer",
                ],
            )

    def test_iter_rows_count_matches_keywords_times_locations(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "cfg.yaml"
            p.write_text(
                """
xing:
  searches:
    - name: xing_data_eng
      keywords:
        - "Data Engineer"
        - "data engineer"
        - "SQL Developer"
        - "Cloud Data Engineer"
      locations:
        - "Berlin"
        - "Munich"
""".lstrip(),
                encoding="utf-8",
            )
            cfg = load_xing_config(p)
            rows = iter_search_definition_rows(cfg)
            # 3 unique keywords x 2 locations
            self.assertEqual(len(rows), 6)


if __name__ == "__main__":
    unittest.main()
