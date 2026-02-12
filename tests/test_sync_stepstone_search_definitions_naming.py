import unittest

from job_scrape.stepstone_config import StepstoneSearchSpec
from scripts.sync_search_definitions_stepstone import build_stepstone_facets
from scripts.sync_search_definitions_stepstone import build_search_definition_name


class TestSyncStepstoneSearchDefinitionsNaming(unittest.TestCase):
    def test_name_is_stable_and_ascii(self):
        name = build_search_definition_name(
            base="de_data_eng_stepstone",
            location="26121 Oldenburg",
            location_idx=0,
            kw_idx=1,
            keyword="Data Platform Engineering",
        )
        self.assertEqual(
            name,
            "de_data_eng_stepstone__loc0_26121_oldenburg__kw1_data_platform_engineering",
        )

    def test_name_changes_by_location_and_keyword_idx(self):
        a = build_search_definition_name(
            base="s",
            location="Berlin",
            location_idx=0,
            kw_idx=0,
            keyword="Data Engineer",
        )
        b = build_search_definition_name(
            base="s",
            location="Hamburg",
            location_idx=1,
            kw_idx=0,
            keyword="Data Engineer",
        )
        c = build_search_definition_name(
            base="s",
            location="Berlin",
            location_idx=0,
            kw_idx=1,
            keyword="Data Engineer",
        )
        self.assertNotEqual(a, b)
        self.assertNotEqual(a, c)

    def test_build_facets_includes_age_days_when_present(self):
        spec = StepstoneSearchSpec(
            name="s",
            keywords=("Data Engineer",),
            locations=("Deutschland",),
            sort=2,
            radius=30,
            where_type="autosuggest",
            search_origin="Resultlist_top-search",
            age_days=1,
        )
        facets = build_stepstone_facets(spec)
        self.assertEqual(facets["age_days"], 1)
        self.assertEqual(facets["sort"], 2)

    def test_build_facets_omits_age_days_when_missing(self):
        spec = StepstoneSearchSpec(
            name="s",
            keywords=("Data Engineer",),
            locations=("Deutschland",),
        )
        facets = build_stepstone_facets(spec)
        self.assertNotIn("age_days", facets)


if __name__ == "__main__":
    unittest.main()
