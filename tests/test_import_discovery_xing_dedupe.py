import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from job_scrape.xing import build_external_job_id, canonicalize_external_job_url
from scripts import import_discovery_xing


class _FakeDB:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}
        self.hits: dict[tuple[str, str], dict] = {}
        self.search_runs: dict[str, dict] = {}
        self.commits = 0


class _FakeCursor:
    def __init__(self, db: _FakeDB) -> None:
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        sql_norm = " ".join(sql.split()).lower()
        if "insert into job_scrape.xing_jobs" in sql_norm:
            self._insert_xing_jobs(params)
            return
        if "insert into job_scrape.xing_job_search_hits" in sql_norm:
            self._insert_xing_hits(params)
            return
        if "update job_scrape.xing_search_runs" in sql_norm:
            self._update_xing_search_runs(params)
            return
        raise AssertionError(f"Unexpected SQL in test: {sql_norm}")

    def _insert_xing_jobs(self, params) -> None:
        (job_id, job_url, is_external, list_preview_json, first_seen_at, last_seen_at, srid) = params
        existing = self.db.jobs.get(job_id)
        new_preview = json.loads(list_preview_json)
        if existing is None:
            self.db.jobs[job_id] = {
                "job_id": job_id,
                "job_url": job_url,
                "is_external": is_external,
                "list_preview": new_preview,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "last_seen_search_run_id": srid,
            }
            return

        merged_preview = dict(existing["list_preview"])
        if not merged_preview:
            merged_preview = new_preview
        else:
            merged_preview.update(new_preview)
        existing.update(
            {
                "job_url": job_url,
                "is_external": is_external,
                "list_preview": merged_preview,
                "last_seen_at": last_seen_at,
                "last_seen_search_run_id": srid,
            }
        )

    def _insert_xing_hits(self, params) -> None:
        srid, job_id, rank, page_start, scraped_at = params
        key = (srid, job_id)
        if key in self.db.hits:
            return
        self.db.hits[key] = {
            "search_run_id": srid,
            "job_id": job_id,
            "rank": rank,
            "page_start": page_start,
            "scraped_at": scraped_at,
        }

    def _update_xing_search_runs(self, params) -> None:
        status, pages_fetched, jobs_discovered, blocked, srid = params
        self.db.search_runs[srid] = {
            "status": status,
            "pages_fetched": pages_fetched,
            "jobs_discovered": jobs_discovered,
            "blocked": blocked,
        }


class _FakeConn:
    def __init__(self, db: _FakeDB) -> None:
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self):
        return _FakeCursor(self.db)

    def commit(self) -> None:
        self.db.commits += 1


class TestImportDiscoveryXingDedupe(unittest.TestCase):
    def test_import_discovery_dedupes_master_jobs_and_keeps_hits_per_search_run(self):
        db = _FakeDB()

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "in.jsonl"
            p.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "record_type": "page_fetch",
                                "crawl_run_id": "crawl-1",
                                "search_run_id": "sr-1",
                                "page_start": 0,
                            }
                        ),
                        json.dumps(
                            {
                                "record_type": "job_discovered",
                                "crawl_run_id": "crawl-1",
                                "search_run_id": "sr-1",
                                "page_start": 0,
                                "job_id": "150143308",
                                "job_url": "https://www.xing.com/jobs/ulm-role-150143308",
                                "rank": 0,
                                "scraped_at": "2026-02-12T10:00:00Z",
                                "list_preview": {"job_title": "Data Engineer"},
                            }
                        ),
                        json.dumps(
                            {
                                "record_type": "page_fetch",
                                "crawl_run_id": "crawl-1",
                                "search_run_id": "sr-2",
                                "page_start": 0,
                            }
                        ),
                        json.dumps(
                            {
                                "record_type": "job_discovered",
                                "crawl_run_id": "crawl-1",
                                "search_run_id": "sr-2",
                                "page_start": 0,
                                "job_id": "150143308",
                                "job_url": "https://www.xing.com/jobs/ulm-role-150143308",
                                "rank": 4,
                                "scraped_at": "2026-02-12T10:01:00Z",
                                "list_preview": {"company_name": "Acme GmbH"},
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            out = io.StringIO()
            with (
                patch.object(import_discovery_xing, "connect", return_value=_FakeConn(db)),
                patch.object(import_discovery_xing.sys, "argv", ["import_discovery_xing.py", str(p)]),
                redirect_stdout(out),
            ):
                import_discovery_xing.main()

        self.assertEqual(len(db.jobs), 1)
        self.assertEqual(len(db.hits), 2)
        self.assertEqual(
            db.jobs["150143308"]["last_seen_at"],
            datetime.fromisoformat("2026-02-12T10:01:00+00:00"),
        )
        self.assertEqual(db.jobs["150143308"]["list_preview"]["job_title"], "Data Engineer")
        self.assertEqual(db.jobs["150143308"]["list_preview"]["company_name"], "Acme GmbH")
        self.assertEqual(db.search_runs["sr-1"]["jobs_discovered"], 1)
        self.assertEqual(db.search_runs["sr-2"]["jobs_discovered"], 1)
        self.assertEqual(db.commits, 1)

    def test_external_url_hash_ignores_query_and_fragment(self):
        a = canonicalize_external_job_url("https://jobs.example.com/role-1?utm=foo#section")
        b = canonicalize_external_job_url("https://jobs.example.com/role-1?src=bar")
        self.assertEqual(a, "https://jobs.example.com/role-1")
        self.assertEqual(a, b)
        self.assertEqual(build_external_job_id(a), build_external_job_id(b))


if __name__ == "__main__":
    unittest.main()
