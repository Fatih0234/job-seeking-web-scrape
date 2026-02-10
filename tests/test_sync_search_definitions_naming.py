import unittest

from scripts.sync_search_definitions import build_search_definition_name


class TestSyncSearchDefinitionsNaming(unittest.TestCase):
    def test_name_is_stable_and_ascii(self):
        name = build_search_definition_name(
            base="de_data_roles",
            country="Germany",
            kw_idx=0,
            keyword="Data Platform Engineer",
        )
        self.assertEqual(name, "de_data_roles__germany__kw0_data_platform_engineer")

    def test_name_changes_by_kw_idx(self):
        a = build_search_definition_name(base="s", country="Germany", kw_idx=0, keyword="Data Engineer")
        b = build_search_definition_name(base="s", country="Germany", kw_idx=1, keyword="Data Engineer")
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()

