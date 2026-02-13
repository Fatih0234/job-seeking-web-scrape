from __future__ import annotations

from scripts.db import connect


SQL = """
create schema if not exists job_scrape;

-- Map-only location expansion layer:
-- - Splits multi-city job_location strings into city tokens (capped)
-- - Emits a remote token when job looks remote-like (or platform indicates remote/hybrid)
create or replace view job_scrape.jobs_dashboard_location_candidates_v as
with base as (
  select
    d.platform,
    d.job_id,
    nullif(btrim(d.job_location), '') as job_location_raw,
    case when d.platform = 'stepstone' then sd.homeoffice_possible else null end as stepstone_homeoffice_possible,
    case when d.platform = 'xing' then xd.work_model else null end as xing_work_model
  from job_scrape.jobs_dashboard_v d
  left join job_scrape.stepstone_job_details sd
    on d.platform = 'stepstone' and sd.job_id = d.job_id
  left join job_scrape.xing_job_details xd
    on d.platform = 'xing' and xd.job_id = d.job_id
),
classified as (
  select
    b.*,
    (
      (b.job_location_raw is not null and b.job_location_raw ~* '(remote|bundesweit|deutschlandweit|home\\s*-?office|homeoffice|wfh|mobil|hybrid)')
      or coalesce(b.stepstone_homeoffice_possible, false)
      or (b.xing_work_model in ('Remote', 'Hybrid'))
    ) as is_remote_like,
    -- Germany-only for now (configurable later)
    'de'::text as country_scope,
    nullif(
      btrim(
        regexp_replace(
          regexp_replace(lower(coalesce(b.job_location_raw, '')), '\\([^\\)]*\\)', ' ', 'g'),
          '\\s+',
          ' ',
          'g'
        )
      ),
      ''
    ) as loc_canon
  from base b
),
raw_tokens as (
  select
    c.platform,
    c.job_id,
    c.job_location_raw,
    c.is_remote_like,
    c.country_scope,
    t.token_raw,
    t.ord
  from classified c
  cross join lateral unnest(regexp_split_to_array(coalesce(c.loc_canon, ''), '\\s*(?:,|/|\\|)\\s*')) with ordinality as t(token_raw, ord)
),
clean_tokens_raw as (
  select
    rt.platform,
    rt.job_id,
    rt.job_location_raw,
    rt.is_remote_like,
    rt.country_scope,
    rt.token_raw,
    rt.ord,
    nullif(
      btrim(
        regexp_replace(
          regexp_replace(
            -- Remove remote-like words inside tokens (e.g. "Berlin Hybrid" -> "berlin")
            regexp_replace(
              lower(coalesce(rt.token_raw, '')),
              '(remote|bundesweit|deutschlandweit|home\\s*-?office|homeoffice|wfh|mobil|hybrid)',
              ' ',
              'g'
            ),
            '\\s+',
            ' ',
            'g'
          ),
          '^[,;\\-\\s\\(\\)\\[\\]]+|[,;\\-\\s\\(\\)\\[\\]]+$',
          '',
          'g'
        )
      ),
      ''
    ) as token_clean
  from raw_tokens rt
),
clean_tokens as (
  select
    ctr.*,
    (ctr.token_clean in (
      -- German states (and common English equivalents) that should not duplicate city markers
      'bavaria', 'bayern',
      'baden-württemberg', 'baden-wurttemberg',
      'north rhine-westphalia', 'nordrhein-westfalen',
      'lower saxony', 'niedersachsen',
      'hesse', 'hessen',
      'saxony', 'sachsen',
      'saxony-anhalt', 'sachsen-anhalt',
      'thuringia', 'thüringen', 'thueringen',
      'rhineland-palatinate', 'rheinland-pfalz',
      'schleswig-holstein',
      'saarland',
      'brandenburg',
      'mecklenburg-vorpommern',
      'mecklenburg western pomerania',
      'greater munich metropolitan area',
      'kanton zürich', 'canton zurich'
    )) as is_admin_region
  from clean_tokens_raw ctr
),
filtered_city_tokens as (
  select
    ct.platform,
    ct.job_id,
    ct.job_location_raw,
    ct.is_remote_like,
    ct.country_scope,
    ct.token_clean as city_token_raw,
    ct.token_clean as city_token_norm,
    ct.is_admin_region,
    min(ct.ord) as ord
  from clean_tokens ct
  where ct.token_clean is not null
    and length(ct.token_clean) >= 2
    and ct.token_clean not in (
      'remote',
      'bundesweit',
      'deutschlandweit',
      'home office',
      'home-office',
      'homeoffice',
      'hybrid',
      'mobil',
      'wfh',
      'de', 'at', 'ch',
      'dach',
      'germany', 'deutschland',
      'austria', 'oesterreich', 'österreich',
      'switzerland', 'schweiz'
    )
  group by
    ct.platform, ct.job_id, ct.job_location_raw, ct.is_remote_like, ct.country_scope, ct.token_clean, ct.is_admin_region
),
city_summary as (
  select
    f.platform,
    f.job_id,
    count(*)::int as map_locations_total
  from filtered_city_tokens f
  group by f.platform, f.job_id
),
city_ranked as (
  select
    f.*,
    max(case when not f.is_admin_region then 1 else 0 end) over (partition by f.platform, f.job_id) as has_non_admin_token,
    row_number() over (partition by f.platform, f.job_id order by f.ord asc, f.city_token_norm asc) as city_token_idx
  from filtered_city_tokens f
),
city_capped as (
  select
    r.platform,
    r.job_id,
    r.job_location_raw,
    r.is_remote_like,
    true as has_city_tokens,
    s.map_locations_total,
    least(s.map_locations_total, 10)::int as map_locations_used,
    r.city_token_idx,
    r.city_token_raw,
    r.city_token_norm,
    'city'::text as token_kind,
    r.country_scope
  from city_ranked r
  join city_summary s
    on s.platform = r.platform and s.job_id = r.job_id
  where r.city_token_idx <= 10
    and (not r.is_admin_region or r.has_non_admin_token = 0)
),
remote_rows as (
  select
    c.platform,
    c.job_id,
    c.job_location_raw,
    c.is_remote_like,
    (coalesce(s.map_locations_total, 0) > 0) as has_city_tokens,
    coalesce(s.map_locations_total, 0)::int as map_locations_total,
    least(coalesce(s.map_locations_total, 0), 10)::int as map_locations_used,
    null::int as city_token_idx,
    null::text as city_token_raw,
    null::text as city_token_norm,
    'remote'::text as token_kind,
    c.country_scope
  from classified c
  left join city_summary s
    on s.platform = c.platform and s.job_id = c.job_id
  where c.is_remote_like
)
select * from city_capped
union all
select * from remote_rows
;

-- Map points view:
-- one row per job per (city token) and/or remote marker.
create or replace view job_scrape.jobs_dashboard_map_points_v as
with c as (
  select * from job_scrape.jobs_dashboard_location_candidates_v
),
joined as (
  select
    d.*,
    c.token_kind,
    c.city_token_idx,
    c.city_token_raw,
    c.city_token_norm,
    c.country_scope,
    c.is_remote_like,
    c.has_city_tokens,
    c.map_locations_total,
    c.map_locations_used,
    (d.platform || ':' || d.job_id || ':' || c.token_kind || ':' || coalesce(c.city_token_idx, 0)::text) as map_point_id,
    case
      when c.token_kind = 'remote' then 'Remote / Bundesweit'
      else initcap(c.city_token_raw)
    end as map_city_label
  from c
  join job_scrape.jobs_dashboard_v d
    on d.platform = c.platform and d.job_id = c.job_id
),
geo as (
  select
    j.*,
    gc.lat as geocode_lat,
    gc.lon as geocode_lon,
    gc.status as geocode_cache_status,
    gc.provider as geocode_cache_provider,
    gc.result_type as geocode_result_type,
    gc.rank_confidence as geocode_rank_confidence,
    gc.rank_importance as geocode_rank_importance,
    gc.formatted as geocode_formatted,
    gc.city as geocode_city,
    gc.state as geocode_state,
    gc.country as geocode_country,
    gc.country_code as geocode_country_code,
    gc.attempt_count as geocode_attempt_count,
    gc.next_retry_at as geocode_next_retry_at,
    gc.last_error as geocode_last_error
  from joined j
  left join job_scrape.location_geocode_cache gc
    on j.token_kind = 'city'
   and gc.provider = 'geoapify'
   and gc.location_text_norm = j.city_token_norm
   and gc.country_scope = j.country_scope
)
select
  g.platform,
  g.job_id,
  g.job_url,
  g.job_title,
  g.company_name,
  g.job_location,
  g.posted_time_ago,
  g.job_description,
  g.scraped_at,
  g.first_seen_at,
  g.last_seen_at,
  g.parse_ok,
  g.last_error,
  g.extracted_skills,
  g.posted_at_utc,
  g.posted_at_source,
  g.posted_at_parse_ok,
  g.posted_at_parse_detail,

  g.token_kind,
  g.map_point_id,
  g.map_city_label,
  g.city_token_idx,
  g.city_token_raw,
  g.city_token_norm,
  g.is_remote_like,
  g.has_city_tokens,
  g.map_locations_total,
  g.map_locations_used,

  -- lat/lon:
  case
    when g.token_kind = 'remote' then
      47.27 + (
        ((hashtextextended(g.platform || ':' || g.job_id || ':remote:lat', 0) & 9223372036854775807)::double precision)
        / 9223372036854775807.0
      ) * (55.06 - 47.27)
    else g.geocode_lat
  end as lat,
  case
    when g.token_kind = 'remote' then
      5.87 + (
        ((hashtextextended(g.platform || ':' || g.job_id || ':remote:lon', 0) & 9223372036854775807)::double precision)
        / 9223372036854775807.0
      ) * (15.04 - 5.87)
    else g.geocode_lon
  end as lon,

  -- geocode status/metadata (city tokens only)
  case
    when g.token_kind = 'remote' then 'remote_scatter'
    when g.city_token_norm is null then 'missing_input'
    when g.geocode_cache_status is null then 'not_cached'
    else g.geocode_cache_status
  end as geocode_status,
  g.country_scope as geocode_country_scope,
  case when g.token_kind = 'remote' then 'synthetic' else coalesce(g.geocode_cache_provider, 'geoapify') end as geocode_provider,
  g.geocode_result_type,
  g.geocode_rank_confidence,
  g.geocode_rank_importance,
  g.geocode_formatted,
  g.geocode_city,
  g.geocode_state,
  g.geocode_country,
  g.geocode_country_code,
  g.geocode_attempt_count,
  g.geocode_next_retry_at,
  g.geocode_last_error,
  (g.token_kind = 'remote') as remote_scatter_ok,

  -- Weighting:
  case
    when g.token_kind = 'remote' then
      case
        when g.has_city_tokens and g.map_locations_used > 0 then 1.0 / (g.map_locations_used + 1)::double precision
        else 1.0
      end
    else
      case
        when g.map_locations_used <= 0 then null
        when g.is_remote_like and g.has_city_tokens then 1.0 / (g.map_locations_used + 1)::double precision
        else 1.0 / g.map_locations_used::double precision
      end
  end as map_weight,

  ((case
    when g.token_kind = 'remote' then true
    else (g.geocode_lat is not null and g.geocode_lon is not null)
  end)) as is_geocoded
from geo g
;

-- Pre-aggregated city bubbles view for fast initial map render.
-- Remote points are intentionally not aggregated (scatter); frontend can query them from map_points_v.
create or replace view job_scrape.jobs_dashboard_city_bubbles_v as
with pts as (
  select *
  from job_scrape.jobs_dashboard_map_points_v
  where token_kind = 'city'
    and lat is not null and lon is not null
),
base as (
  select
    p.*,
    coalesce(p.posted_at_utc, p.first_seen_at) as event_ts,
    coalesce(nullif(p.geocode_city, ''), p.map_city_label) as bubble_label
  from pts p
),
agg as (
  select
    token_kind,
    lat,
    lon,
    bubble_label as map_city_label,
    count(distinct (platform, job_id))::bigint as jobs_unique,
    sum(map_weight)::double precision as jobs_weighted,
    count(distinct (platform, job_id)) filter (where event_ts >= now() - interval '24 hours')::bigint as new_24h_unique,
    count(distinct (platform, job_id)) filter (where event_ts >= now() - interval '7 days')::bigint as new_7d_unique
  from base
  group by token_kind, lat, lon, bubble_label
)
select
  a.*,
  tc.top_companies,
  ts.top_skills
from agg a
left join lateral (
  select array_agg(company_name order by cnt desc, company_name asc) as top_companies
  from (
    select
      b.company_name,
      count(distinct (b.platform, b.job_id))::bigint as cnt
    from base b
    where b.lat = a.lat and b.lon = a.lon and b.bubble_label = a.map_city_label
      and b.company_name is not null and btrim(b.company_name) <> ''
    group by b.company_name
    order by cnt desc, b.company_name asc
    limit 5
  ) s
) tc on true
left join lateral (
  select array_agg(skill order by cnt desc, skill asc) as top_skills
  from (
    select
      skill,
      count(distinct (b.platform, b.job_id))::bigint as cnt
    from base b
    cross join lateral (
      select jsonb_array_elements_text(e.value) as skill
      from jsonb_each(coalesce(b.extracted_skills, '{}'::jsonb)) as e
      where jsonb_typeof(e.value) = 'array'
    ) sk
    where b.lat = a.lat and b.lon = a.lon and b.bubble_label = a.map_city_label
      and skill is not null and btrim(skill) <> ''
    group by skill
    order by cnt desc, skill asc
    limit 10
  ) s
) ts on true
;

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
