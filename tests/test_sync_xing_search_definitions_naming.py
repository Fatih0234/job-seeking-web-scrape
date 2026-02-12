import unittest

from scripts.sync_search_definitions_xing import build_search_definition_name


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


if __name__ == "__main__":
    unittest.main()

