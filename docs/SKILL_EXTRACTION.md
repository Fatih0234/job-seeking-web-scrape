# Skill Extraction (Deterministic Taxonomy)

This repo supports deterministic extraction of grouped skills/keywords from job descriptions.

Source taxonomy:
- `configs/data-engineering-keyword-taxonomy.yaml`

Extractor:
- `job_scrape/skill_extraction.py`

## Storage (Supabase Postgres)

Skills are stored on `job_scrape.job_details` (because `job_description` lives there).

Apply this migration in Supabase SQL editor:

```sql
alter table job_scrape.job_details
  add column if not exists extracted_skills jsonb,
  add column if not exists extracted_skills_version int,
  add column if not exists extracted_skills_extracted_at timestamptz;
```

Notes:
- `extracted_skills` is a JSON object: `{ "<group>": ["Canonical Skill", ...], ... }`
- `extracted_skills_version` comes from `taxonomy.version` in the YAML file.
- If the columns are not present, `scripts/import_details.py` will still work, but it will skip writing skills.

## Backfill / Recompute

To recompute skills for recent rows that are missing skills (or have an older taxonomy version):

```bash
export SUPABASE_DB_URL="..."
./.venv/bin/python scripts/extract_skills.py
```

Env vars:
- `SKILL_EXTRACT_LIMIT` (default `500`)
- `SKILL_EXTRACT_ONLY_MISSING` (default `1`)
