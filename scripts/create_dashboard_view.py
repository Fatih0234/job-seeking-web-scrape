from __future__ import annotations

from scripts.db import connect


SQL = """
create schema if not exists job_scrape;

create or replace view job_scrape.jobs_dashboard_v as
with base_rows as (
  select
    'linkedin'::text as platform,
    j.job_id,
    j.job_url,
    d.job_title,
    d.company_name,
    d.job_location,
    d.posted_time_ago,
    d.job_description,
    d.scraped_at,
    j.first_seen_at,
    j.last_seen_at,
    d.parse_ok,
    d.last_error,
    d.extracted_skills,
    null::timestamptz as native_posted_at_utc
  from job_scrape.jobs j
  left join job_scrape.job_details d
    on d.source = j.source and d.job_id = j.job_id
  where j.source = 'linkedin'

  union all

  select
    'stepstone'::text as platform,
    j.job_id,
    j.job_url,
    d.job_title,
    d.company_name,
    d.job_location,
    d.posted_time_ago,
    d.job_description,
    d.scraped_at,
    j.first_seen_at,
    j.last_seen_at,
    d.parse_ok,
    d.last_error,
    d.extracted_skills,
    null::timestamptz as native_posted_at_utc
  from job_scrape.stepstone_jobs j
  left join job_scrape.stepstone_job_details d
    on d.job_id = j.job_id

  union all

  select
    'xing'::text as platform,
    j.job_id,
    j.job_url,
    d.job_title,
    d.company_name,
    d.job_location,
    d.posted_time_ago,
    d.job_description,
    d.scraped_at,
    j.first_seen_at,
    j.last_seen_at,
    d.parse_ok,
    d.last_error,
    d.extracted_skills,
    d.posted_at_utc as native_posted_at_utc
  from job_scrape.xing_jobs j
  left join job_scrape.xing_job_details d
    on d.job_id = j.job_id
),
normalized as (
  select
    b.*,
    nullif(trim(b.posted_time_ago), '') as posted_time_trimmed,
    nullif(
      lower(
        trim(
          regexp_replace(
            coalesce(b.posted_time_ago, ''),
            '^\\s*published:\\s*',
            '',
            'i'
          )
        )
      ),
      ''
    ) as posted_time_norm
  from base_rows b
),
matched as (
  select
    n.*,
    regexp_match(
      n.posted_time_norm,
      '^([0-9]+)\\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\\s+ago$'
    ) as en_match,
    regexp_match(
      n.posted_time_norm,
      '^vor\\s+([0-9]+)\\s+(minute|minuten|stunde|stunden|tag|tagen|woche|wochen|monat|monaten|jahr|jahren)$'
    ) as de_match
  from normalized n
),
with_delta as (
  select
    m.*,
    case
      when m.posted_time_norm in ('today', 'heute', 'just now', 'gerade eben') then make_interval()
      when m.posted_time_norm in ('yesterday', 'gestern') then make_interval(days => 1)
      when m.en_match is not null then
        case m.en_match[2]
          when 'minute' then make_interval(mins => m.en_match[1]::int)
          when 'minutes' then make_interval(mins => m.en_match[1]::int)
          when 'hour' then make_interval(hours => m.en_match[1]::int)
          when 'hours' then make_interval(hours => m.en_match[1]::int)
          when 'day' then make_interval(days => m.en_match[1]::int)
          when 'days' then make_interval(days => m.en_match[1]::int)
          when 'week' then make_interval(days => 7 * m.en_match[1]::int)
          when 'weeks' then make_interval(days => 7 * m.en_match[1]::int)
          when 'month' then make_interval(months => m.en_match[1]::int)
          when 'months' then make_interval(months => m.en_match[1]::int)
          when 'year' then make_interval(years => m.en_match[1]::int)
          when 'years' then make_interval(years => m.en_match[1]::int)
          else null
        end
      when m.de_match is not null then
        case m.de_match[2]
          when 'minute' then make_interval(mins => m.de_match[1]::int)
          when 'minuten' then make_interval(mins => m.de_match[1]::int)
          when 'stunde' then make_interval(hours => m.de_match[1]::int)
          when 'stunden' then make_interval(hours => m.de_match[1]::int)
          when 'tag' then make_interval(days => m.de_match[1]::int)
          when 'tagen' then make_interval(days => m.de_match[1]::int)
          when 'woche' then make_interval(days => 7 * m.de_match[1]::int)
          when 'wochen' then make_interval(days => 7 * m.de_match[1]::int)
          when 'monat' then make_interval(months => m.de_match[1]::int)
          when 'monaten' then make_interval(months => m.de_match[1]::int)
          when 'jahr' then make_interval(years => m.de_match[1]::int)
          when 'jahren' then make_interval(years => m.de_match[1]::int)
          else null
        end
      else null
    end as posted_delta
  from matched m
)
select
  d.platform,
  d.job_id,
  d.job_url,
  d.job_title,
  d.company_name,
  d.job_location,
  d.posted_time_ago,
  d.job_description,
  d.scraped_at,
  d.first_seen_at,
  d.last_seen_at,
  d.parse_ok,
  d.last_error,
  d.extracted_skills,
  case
    when d.platform = 'xing' and d.native_posted_at_utc is not null then d.native_posted_at_utc
    when d.scraped_at is not null and d.posted_delta is not null then d.scraped_at - d.posted_delta
    else null
  end as posted_at_utc,
  case
    when d.platform = 'xing' and d.native_posted_at_utc is not null then 'native_xing_posted_at_utc'
    when d.posted_time_norm is null then 'missing_input'
    when d.posted_time_norm in ('today', 'heute', 'just now', 'gerade eben', 'yesterday', 'gestern') then 'parsed_keyword'
    when d.en_match is not null then 'parsed_relative_en'
    when d.de_match is not null then 'parsed_relative_de'
    else 'unparsed_format'
  end as posted_at_source,
  case
    when d.platform = 'xing' and d.native_posted_at_utc is not null then true
    when d.scraped_at is not null and d.posted_delta is not null then true
    else false
  end as posted_at_parse_ok,
  case
    when d.platform = 'xing' and d.native_posted_at_utc is not null then 'posted_at_utc'
    when d.posted_time_norm is null then 'missing_input'
    when d.posted_time_norm in ('today', 'heute', 'just now', 'gerade eben', 'yesterday', 'gestern') then d.posted_time_norm
    when d.en_match is not null then d.en_match[2]
    when d.de_match is not null then d.de_match[2]
    else d.posted_time_norm
  end as posted_at_parse_detail
from with_delta d
;
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()
    print("jobs_dashboard_view_ready")


if __name__ == "__main__":
    main()
