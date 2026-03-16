import unittest

import psycopg

from scripts.ensure_lifecycle_schema import _is_timeout_error


class TestEnsureLifecycleSchemaTimeoutDetection(unittest.TestCase):
    def test_query_canceled_is_timeout_error(self):
        self.assertTrue(_is_timeout_error(psycopg.errors.QueryCanceled()))

    def test_lock_not_available_is_timeout_error(self):
        self.assertTrue(_is_timeout_error(psycopg.errors.LockNotAvailable()))

    def test_non_timeout_error_is_not_timeout_error(self):
        self.assertFalse(_is_timeout_error(RuntimeError("boom")))


if __name__ == "__main__":
    unittest.main()
