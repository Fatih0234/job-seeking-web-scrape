from __future__ import annotations

from scripts.db import connect


SQL = """
create schema if not exists job_scrape;

create or replace view job_scrape.jobs_dashboard_map_v as
with base as (
  select
    d.*,
    nullif(trim(d.job_location), '') as location_text_raw,
    nullif(
      lower(
        trim(
          regexp_replace(
            regexp_replace(coalesce(d.job_location, ''), '\\s+', ' ', 'g'),
            '\\s*,\\s*',
            ', ',
            'g'
          )
        )
      ),
      ''
    ) as location_text_norm,
    case
      when d.platform in ('linkedin', 'stepstone') then 'de'
      when d.platform = 'xing' then 'de,at,ch'
      else 'de,at,ch'
    end as geocode_country_scope
  from job_scrape.jobs_dashboard_v d
)
select
  b.platform,
  b.job_id,
  b.job_url,
  b.job_title,
  b.company_name,
  b.job_location,
  b.posted_time_ago,
  b.job_description,
  b.scraped_at,
  b.first_seen_at,
  b.last_seen_at,
  b.parse_ok,
  b.last_error,
  b.extracted_skills,
  b.posted_at_utc,
  b.posted_at_source,
  b.posted_at_parse_ok,
  b.posted_at_parse_detail,
  c.lat,
  c.lon,
  case
    when b.location_text_norm is null then 'missing_input'
    when c.status is null then 'not_cached'
    else c.status
  end as geocode_status,
  b.geocode_country_scope,
  coalesce(c.provider, 'geoapify') as geocode_provider,
  c.result_type as geocode_result_type,
  c.rank_confidence as geocode_rank_confidence,
  c.rank_importance as geocode_rank_importance,
  c.formatted as geocode_formatted,
  c.city as geocode_city,
  c.state as geocode_state,
  c.country as geocode_country,
  c.country_code as geocode_country_code,
  c.attempt_count as geocode_attempt_count,
  c.next_retry_at as geocode_next_retry_at,
  c.last_error as geocode_last_error,
  (c.lat is not null and c.lon is not null) as is_geocoded
from base b
left join job_scrape.location_geocode_cache c
  on c.provider = 'geoapify'
 and c.location_text_norm = b.location_text_norm
 and c.country_scope = b.geocode_country_scope
;
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()
    print("jobs_dashboard_map_view_ready")


if __name__ == "__main__":
    main()
