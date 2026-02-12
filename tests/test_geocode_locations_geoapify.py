import unittest
from unittest.mock import Mock, patch

from scripts.geocode_locations_geoapify import (
    CacheRow,
    build_result_map,
    compute_retry_delay_minutes,
    country_scope_for_platform,
    create_batch_job,
    normalize_location_text,
    parse_batch_payload,
    poll_batch_results,
    unresolved_status,
    _resolve_result_for_row,
)


class TestGeocodeLocationsGeoapify(unittest.TestCase):
    def test_normalize_location_text(self):
        self.assertEqual(normalize_location_text("Berlin"), "berlin")
        self.assertEqual(
            normalize_location_text(" Oldenburg   , Lower   Saxony "),
            "oldenburg, lower saxony",
        )

    def test_country_scope_for_platform(self):
        self.assertEqual(country_scope_for_platform("linkedin"), "de")
        self.assertEqual(country_scope_for_platform("stepstone"), "de")
        self.assertEqual(country_scope_for_platform("xing"), "de,at,ch")

    def test_retry_delay_backoff_capped(self):
        self.assertEqual(
            compute_retry_delay_minutes(attempt_after=1, base_minutes=60, max_minutes=10080),
            60,
        )
        self.assertEqual(
            compute_retry_delay_minutes(attempt_after=2, base_minutes=60, max_minutes=10080),
            120,
        )
        self.assertEqual(
            compute_retry_delay_minutes(attempt_after=20, base_minutes=60, max_minutes=10080),
            10080,
        )

    def test_unresolved_status_transition(self):
        self.assertEqual(unresolved_status(attempt_after=1, max_attempts=6, kind="no_match"), "no_match")
        self.assertEqual(unresolved_status(attempt_after=1, max_attempts=6, kind="error"), "error")
        self.assertEqual(
            unresolved_status(attempt_after=6, max_attempts=6, kind="no_match"),
            "failed_permanent",
        )
        self.assertEqual(
            unresolved_status(attempt_after=8, max_attempts=6, kind="error"),
            "failed_permanent",
        )

    def test_parse_batch_payload_variants(self):
        status, rows, err = parse_batch_payload([{"query": {"text": "Berlin"}, "lat": 1, "lon": 2}])
        self.assertEqual(status, "finished")
        self.assertEqual(len(rows), 1)
        self.assertIsNone(err)

        status, rows, err = parse_batch_payload({"status": "pending", "id": "abc"})
        self.assertEqual(status, "pending")
        self.assertEqual(rows, [])
        self.assertIsNone(err)

        status, rows, err = parse_batch_payload({"status": "error", "message": "bad"})
        self.assertEqual(status, "error")
        self.assertEqual(rows, [])
        self.assertIsNotNone(err)

    def test_build_result_map_and_row_resolution(self):
        rows = [
            CacheRow(id="1", location_text_raw="Berlin", location_text_norm="berlin", country_scope="de", attempt_count=0),
            CacheRow(
                id="2",
                location_text_raw="Oldenburg, Lower Saxony",
                location_text_norm="oldenburg, lower saxony",
                country_scope="de",
                attempt_count=0,
            ),
        ]
        results = [
            {"query": {"text": "Berlin"}, "lat": 52.5, "lon": 13.4},
            {"query": {"text": "Oldenburg, Lower Saxony"}, "lat": 53.1, "lon": 8.2},
        ]
        rmap = build_result_map(results)
        self.assertIn("berlin", rmap)
        self.assertIn("oldenburg, lower saxony", rmap)

        item0 = _resolve_result_for_row(row=rows[0], idx=0, rows_in_batch=rows, results=results, result_map=rmap)
        self.assertIsNotNone(item0)
        self.assertEqual(item0["lat"], 52.5)

    def test_poll_batch_results_timeout(self):
        session = Mock()
        resp = Mock()
        resp.json.return_value = {"status": "pending", "id": "abc"}
        resp.raise_for_status.return_value = None
        session.get.return_value = resp

        with (
            patch("scripts.geocode_locations_geoapify.time.sleep", return_value=None),
            patch(
                "scripts.geocode_locations_geoapify.time.monotonic",
                side_effect=[0.0, 0.0, 999.0, 999.0],
            ),
        ):
            with self.assertRaises(TimeoutError):
                poll_batch_results(
                    session=session,
                    api_key="k",
                    job_id="abc",
                    poll_seconds=1,
                    poll_timeout_seconds=5,
                )

    def test_create_batch_job_uses_country_filter(self):
        session = Mock()
        resp = Mock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"id": "job1", "status": "pending"}
        session.post.return_value = resp

        job_id, immediate = create_batch_job(
            session=session,
            api_key="k",
            country_scope="de,at,ch",
            addresses=["Berlin", "Vienna"],
        )
        self.assertEqual(job_id, "job1")
        self.assertIsNone(immediate)
        call_kwargs = session.post.call_args.kwargs
        self.assertEqual(call_kwargs["params"]["filter"], "countrycode:de,at,ch")
        self.assertEqual(call_kwargs["params"]["limit"], 1)
        self.assertEqual(call_kwargs["json"], ["Berlin", "Vienna"])


if __name__ == "__main__":
    unittest.main()
