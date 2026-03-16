from __future__ import annotations

import unittest

from job_scrape.xing_block_detection import looks_blocked


class TestXingBlockDetection(unittest.TestCase):
    def test_blocks_on_status_codes(self):
        self.assertTrue(looks_blocked(status=403, body="ok"))
        self.assertTrue(looks_blocked(status=429, body="ok"))
        self.assertTrue(looks_blocked(status=503, body="ok"))

    def test_blocks_on_known_markers(self):
        self.assertTrue(looks_blocked(status=200, body="Access denied"))
        self.assertTrue(looks_blocked(status=200, body="Please verify you are a human"))
        self.assertTrue(looks_blocked(status=200, body="captcha"))
        self.assertTrue(looks_blocked(status=200, body="errors.edgesuite.net"))

    def test_not_blocked_for_normal_pages(self):
        self.assertFalse(
            looks_blocked(status=200, body="<html><title>Jobs</title></html>")
        )


if __name__ == "__main__":
    unittest.main()
