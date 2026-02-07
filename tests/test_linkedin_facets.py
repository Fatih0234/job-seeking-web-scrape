import unittest

from job_scrape.linkedin_facets import build_label_to_value_map, parse_facet_options, resolve_facet_values


_HTML = """
<form id="jserp-filters">
  <input id="f_JT-0" form="jserp-filters" name="f_JT" value="F" type="checkbox">
  <label for="f_JT-0">Full-time (123)</label>
  <input id="f_JT-1" form="jserp-filters" name="f_JT" value="P" type="checkbox">
  <label for="f_JT-1">Part-time (45)</label>

  <input id="f_E-0" form="jserp-filters" name="f_E" value="2" type="checkbox">
  <label for="f_E-0">Entry level (99)</label>

  <input id="f_WT-0" form="jserp-filters" name="f_WT" value="2" type="checkbox">
  <label for="f_WT-0">Remote (7)</label>

  <input id="f_TPR-0" form="jserp-filters" name="f_TPR" checked value type="radio">
  <label for="f_TPR-0">Any time (999)</label>
  <input id="f_TPR-1" form="jserp-filters" name="f_TPR" value="r604800" type="radio">
  <label for="f_TPR-1">Past week (12)</label>
</form>
"""


class TestLinkedInFacets(unittest.TestCase):
    def test_parse_and_resolve(self):
        opts = parse_facet_options(_HTML)
        label_map = build_label_to_value_map(opts)

        self.assertEqual(resolve_facet_values(label_map, facet="f_JT", requested_labels=["Full-time"]), ["F"])
        self.assertEqual(resolve_facet_values(label_map, facet="f_E", requested_labels=["Entry level"]), ["2"])
        self.assertEqual(resolve_facet_values(label_map, facet="f_WT", requested_labels=["Remote"]), ["2"])
        self.assertEqual(resolve_facet_values(label_map, facet="f_TPR", requested_labels=["Past week"]), ["r604800"])

    def test_accept_raw_codes(self):
        opts = parse_facet_options(_HTML)
        label_map = build_label_to_value_map(opts)

        self.assertEqual(resolve_facet_values(label_map, facet="f_JT", requested_labels=["F"]), ["F"])
        self.assertEqual(resolve_facet_values(label_map, facet="f_TPR", requested_labels=["r604800"]), ["r604800"])


if __name__ == "__main__":
    unittest.main()

