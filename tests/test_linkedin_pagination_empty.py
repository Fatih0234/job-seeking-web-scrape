import unittest

from job_scrape.linkedin_pagination import parse_see_more_fragment


class TestLinkedInPaginationEmpty(unittest.TestCase):
    def test_empty_fragment_parses_to_empty_list(self):
        self.assertEqual(parse_see_more_fragment("", search_url="https://example.com"), [])


if __name__ == "__main__":
    unittest.main()

