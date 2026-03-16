from __future__ import annotations

import re
from typing import Any

from scripts.db import connect


WORKING_STUDENT_TITLE_TERMS: tuple[str, ...] = (
    r"werkstudent(?:[:*/._-]?in|en|entätigkeit)?",
    r"studentenjobs?",
    r"student job",
    r"working[ -]?student",
    r"student assistant",
    r"student employee",
    r"student worker",
    r"student(?:ische|ischer|isches)?[ -]+(?:hilfskraft|aushilfe|mitarbeiter(?:[:*/._-]?in)?)",
)

WORKING_STUDENT_STRUCTURED_TERMS: tuple[str, ...] = WORKING_STUDENT_TITLE_TERMS + (
    r"industrial placement student",
)

INTERNSHIP_TERMS: tuple[str, ...] = (
    r"praktikum",
    r"praktikant(?:[:*/._-]?in)?",
    r"pflichtpraktikum",
    r"freiwillig(?:es|en)?[ -]+praktikum",
    r"internship",
    r"intern",
    r"trainee intern",
    r"praxissemester",
)

PART_TIME_TERMS: tuple[str, ...] = (
    r"teilzeit(?:stelle|job)?",
    r"part[ -]?time",
)

XING_WORKING_STUDENT_VALUES: tuple[str, ...] = ("student", "for students")
LINKEDIN_INTERNSHIP_VALUES: tuple[str, ...] = ("internship",)
LINKEDIN_PART_TIME_VALUES: tuple[str, ...] = ("part-time", "part time")
XING_PART_TIME_VALUES: tuple[str, ...] = ("part-time", "part time")

CONTRADICTORY_STRUCTURED_TERMS: tuple[str, ...] = (
    r"full[ -]?time",
    r"vollzeit",
    r"permanent contract",
    r"feste anstellung",
    r"contract",
    r"self-employed",
    r"temporary contract",
    r"befristeter vertrag",
)


def _postgres_regex_union(terms: tuple[str, ...]) -> str:
    return r"\m(?:" + "|".join(terms) + r")\M"


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


def _python_regex_union(terms: tuple[str, ...]) -> str:
    return r"(?<!\w)(?:" + "|".join(terms) + r")(?!\w)"


def _compile_pattern(terms: tuple[str, ...]) -> re.Pattern[str]:
    return re.compile(_python_regex_union(terms), re.IGNORECASE)


WORKING_STUDENT_STRUCTURED_SQL_PATTERN = _postgres_regex_union(WORKING_STUDENT_STRUCTURED_TERMS)
WORKING_STUDENT_TITLE_SQL_PATTERN = _postgres_regex_union(WORKING_STUDENT_TITLE_TERMS)
INTERNSHIP_SQL_PATTERN = _postgres_regex_union(INTERNSHIP_TERMS)
PART_TIME_SQL_PATTERN = _postgres_regex_union(PART_TIME_TERMS)
CONTRADICTORY_STRUCTURED_SQL_PATTERN = _postgres_regex_union(CONTRADICTORY_STRUCTURED_TERMS)

WORKING_STUDENT_STRUCTURED_PATTERN = _compile_pattern(WORKING_STUDENT_STRUCTURED_TERMS)
WORKING_STUDENT_TITLE_PATTERN = _compile_pattern(WORKING_STUDENT_TITLE_TERMS)
WORKING_STUDENT_DESCRIPTION_PATTERN = _compile_pattern(WORKING_STUDENT_TITLE_TERMS)
INTERNSHIP_PATTERN = _compile_pattern(INTERNSHIP_TERMS)
PART_TIME_PATTERN = _compile_pattern(PART_TIME_TERMS)
CONTRADICTORY_STRUCTURED_PATTERN = _compile_pattern(CONTRADICTORY_STRUCTURED_TERMS)


def classify_preview(
    *,
    platform: str = "",
    employment_type: str | None = None,
    contract_type: str | None = None,
    work_type: str | None = None,
    part_time_flag: bool | None = None,
    job_title: str | None = None,
    job_description: str | None = None,
) -> dict[str, Any]:
    structured_text = " | ".join(
        value
        for value in (employment_type, contract_type, work_type)
        if value and value.strip()
    )
    structured_norm = _normalize_text(structured_text)
    title_norm = _normalize_text(job_title)
    description_norm = _normalize_text(job_description)
    employment_norm = _normalize_text(employment_type)

    ws_structured_terms = bool(WORKING_STUDENT_STRUCTURED_PATTERN.search(structured_norm))
    ws_xing_employment = platform == "xing" and employment_norm in XING_WORKING_STUDENT_VALUES
    ws_title = bool(WORKING_STUDENT_TITLE_PATTERN.search(title_norm))
    ws_description = bool(WORKING_STUDENT_DESCRIPTION_PATTERN.search(description_norm))

    internship_structured_terms = bool(INTERNSHIP_PATTERN.search(structured_norm))
    internship_linkedin_employment = platform == "linkedin" and employment_norm in LINKEDIN_INTERNSHIP_VALUES
    internship_title = bool(INTERNSHIP_PATTERN.search(title_norm))
    internship_description = bool(INTERNSHIP_PATTERN.search(description_norm))

    part_time_structured_terms = bool(PART_TIME_PATTERN.search(structured_norm))
    part_time_linkedin_employment = platform == "linkedin" and employment_norm in LINKEDIN_PART_TIME_VALUES
    part_time_xing_employment = platform == "xing" and employment_norm in XING_PART_TIME_VALUES
    explicit_part_time_flag = part_time_flag is True
    part_time_title = bool(PART_TIME_PATTERN.search(title_norm))
    part_time_description = bool(PART_TIME_PATTERN.search(description_norm))
    ws_high_signal = ws_structured_terms or ws_xing_employment or ws_title
    internship_high_signal = internship_structured_terms or internship_linkedin_employment or internship_title
    part_time_high_signal = (
        explicit_part_time_flag
        or part_time_structured_terms
        or part_time_linkedin_employment
        or part_time_xing_employment
        or part_time_title
    )

    ws_description_eligible = ws_description and not ws_high_signal and not (internship_high_signal or part_time_high_signal)
    internship_description_eligible = (
        internship_description
        and not internship_high_signal
        and not (ws_high_signal or part_time_high_signal)
    )
    is_working_student = ws_high_signal or ws_description_eligible
    is_internship = internship_high_signal or internship_description_eligible
    is_part_time = part_time_high_signal or part_time_description

    any_structured = (
        ws_structured_terms
        or ws_xing_employment
        or internship_structured_terms
        or internship_linkedin_employment
        or part_time_structured_terms
        or part_time_linkedin_employment
        or part_time_xing_employment
        or explicit_part_time_flag
    )
    any_title = ws_title or internship_title or part_time_title
    any_description = ws_description_eligible or internship_description_eligible or part_time_description

    contradictory_structured = bool(CONTRADICTORY_STRUCTURED_PATTERN.search(structured_norm))
    ws_description_only = ws_description_eligible
    internship_description_only = internship_description_eligible
    part_time_description_only = (
        is_part_time
        and not (
            explicit_part_time_flag
            or part_time_structured_terms
            or part_time_linkedin_employment
            or part_time_xing_employment
            or part_time_title
        )
        and part_time_description
    )

    medium_match = (
        part_time_description_only
        or (ws_description_only and not contradictory_structured)
        or (internship_description_only and not contradictory_structured)
    )
    is_target_role = is_working_student or is_internship or is_part_time

    if not is_target_role:
        confidence = None
        confidence_rank = None
    elif any_structured or any_title:
        confidence = "high"
        confidence_rank = 3
    elif medium_match:
        confidence = "medium"
        confidence_rank = 2
    else:
        confidence = "low"
        confidence_rank = 1

    match_sources = []
    if any_structured:
        match_sources.append("structured")
    if any_title:
        match_sources.append("title")
    if any_description:
        match_sources.append("description")

    match_reasons = []
    if ws_structured_terms:
        match_reasons.append("working_student.structured_terms")
    if ws_xing_employment:
        match_reasons.append("working_student.xing_employment_type")
    if ws_title:
        match_reasons.append("working_student.title_terms")
    if ws_description_eligible:
        match_reasons.append("working_student.description_terms")
    if internship_structured_terms:
        match_reasons.append("internship.structured_terms")
    if internship_linkedin_employment:
        match_reasons.append("internship.linkedin_employment_type")
    if internship_title:
        match_reasons.append("internship.title_terms")
    if internship_description_eligible:
        match_reasons.append("internship.description_terms")
    if explicit_part_time_flag:
        match_reasons.append("part_time.part_time_flag")
    if part_time_structured_terms:
        match_reasons.append("part_time.structured_terms")
    if part_time_linkedin_employment:
        match_reasons.append("part_time.linkedin_employment_type")
    if part_time_xing_employment:
        match_reasons.append("part_time.xing_employment_type")
    if part_time_title:
        match_reasons.append("part_time.title_terms")
    if part_time_description:
        match_reasons.append("part_time.description_terms")

    return {
        "is_working_student": is_working_student,
        "is_internship": is_internship,
        "is_part_time": is_part_time,
        "is_target_role": is_target_role,
        "target_role_confidence": confidence,
        "target_role_confidence_rank": confidence_rank,
        "match_sources": match_sources,
        "match_reasons": match_reasons,
        "has_contradictory_structured_signal": contradictory_structured,
    }


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_text_array(values: tuple[str, ...]) -> str:
    return "array[" + ", ".join(_sql_literal(value) for value in values) + "]::text[]"


def _sql_regex(pattern: str) -> str:
    return _sql_literal(pattern)


SQL = f"""
create schema if not exists job_scrape;

set statement_timeout = '30min';

create or replace view job_scrape.jobs_target_classification_v as
with base as (
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
    d.posted_at_utc,
    d.posted_at_source,
    d.posted_at_parse_ok,
    d.posted_at_parse_detail,
    case
      when d.platform = 'linkedin' then lj.is_active
      when d.platform = 'stepstone' then sj.is_active
      when d.platform = 'xing' then xj.is_active
      else null
    end as is_active,
    case
      when d.platform = 'linkedin' then nullif(trim(ld.criteria->>'employment_type'), '')
      when d.platform = 'xing' then nullif(trim(xd.employment_type), '')
      else null
    end as employment_type,
    case when d.platform = 'stepstone' then nullif(trim(sd.contract_type), '') else null end as contract_type,
    case when d.platform = 'stepstone' then nullif(trim(sd.work_type), '') else null end as work_type,
    case
      when d.platform = 'stepstone' then sd.part_time
      when d.platform = 'linkedin' and nullif(trim(ld.criteria->>'employment_type'), '') is not null
        then lower(trim(ld.criteria->>'employment_type')) = any ({_sql_text_array(LINKEDIN_PART_TIME_VALUES)})
      when d.platform = 'xing' and nullif(trim(xd.employment_type), '') is not null
        then lower(trim(xd.employment_type)) = any ({_sql_text_array(XING_PART_TIME_VALUES)})
      else null
    end as part_time_flag
  from job_scrape.jobs_dashboard_v d
  left join job_scrape.jobs lj
    on d.platform = 'linkedin' and lj.source = 'linkedin' and lj.job_id = d.job_id
  left join job_scrape.job_details ld
    on d.platform = 'linkedin' and ld.source = 'linkedin' and ld.job_id = d.job_id
  left join job_scrape.stepstone_jobs sj
    on d.platform = 'stepstone' and sj.job_id = d.job_id
  left join job_scrape.stepstone_job_details sd
    on d.platform = 'stepstone' and sd.job_id = d.job_id
  left join job_scrape.xing_jobs xj
    on d.platform = 'xing' and xj.job_id = d.job_id
  left join job_scrape.xing_job_details xd
    on d.platform = 'xing' and xd.job_id = d.job_id
),
normalized as (
  select
    b.*,
    lower(coalesce(b.job_title, '')) as job_title_lc,
    lower(coalesce(b.job_description, '')) as job_description_lc,
    lower(coalesce(b.employment_type, '')) as employment_type_lc,
    lower(coalesce(b.contract_type, '')) as contract_type_lc,
    lower(coalesce(b.work_type, '')) as work_type_lc,
    lower(concat_ws(' | ', b.employment_type, b.contract_type, b.work_type)) as structured_text_lc
  from base b
),
flags as (
  select
    n.*,
    n.structured_text_lc ~* {_sql_regex(WORKING_STUDENT_STRUCTURED_SQL_PATTERN)} as ws_structured_terms_match,
    (n.platform = 'xing' and n.employment_type_lc = any ({_sql_text_array(XING_WORKING_STUDENT_VALUES)})) as ws_xing_employment_match,
    n.job_title_lc ~* {_sql_regex(WORKING_STUDENT_TITLE_SQL_PATTERN)} as ws_title_match,
    n.job_description_lc ~* {_sql_regex(WORKING_STUDENT_TITLE_SQL_PATTERN)} as ws_description_match,
    n.structured_text_lc ~* {_sql_regex(INTERNSHIP_SQL_PATTERN)} as internship_structured_terms_match,
    (n.platform = 'linkedin' and n.employment_type_lc = any ({_sql_text_array(LINKEDIN_INTERNSHIP_VALUES)})) as internship_linkedin_employment_match,
    n.job_title_lc ~* {_sql_regex(INTERNSHIP_SQL_PATTERN)} as internship_title_match,
    n.job_description_lc ~* {_sql_regex(INTERNSHIP_SQL_PATTERN)} as internship_description_match,
    (n.part_time_flag is true) as explicit_part_time_flag_match,
    n.structured_text_lc ~* {_sql_regex(PART_TIME_SQL_PATTERN)} as part_time_structured_terms_match,
    (n.platform = 'linkedin' and n.employment_type_lc = any ({_sql_text_array(LINKEDIN_PART_TIME_VALUES)})) as part_time_linkedin_employment_match,
    (n.platform = 'xing' and n.employment_type_lc = any ({_sql_text_array(XING_PART_TIME_VALUES)})) as part_time_xing_employment_match,
    n.job_title_lc ~* {_sql_regex(PART_TIME_SQL_PATTERN)} as part_time_title_match,
    n.job_description_lc ~* {_sql_regex(PART_TIME_SQL_PATTERN)} as part_time_description_match,
    n.structured_text_lc ~* {_sql_regex(CONTRADICTORY_STRUCTURED_SQL_PATTERN)} as contradictory_structured_match
  from normalized n
),
classified as (
  select
    f.*,
    (
      f.ws_structured_terms_match
      or f.ws_xing_employment_match
      or f.ws_title_match
    ) as ws_high_signal,
    (
      f.internship_structured_terms_match
      or f.internship_linkedin_employment_match
      or f.internship_title_match
    ) as internship_high_signal,
    (
      f.explicit_part_time_flag_match
      or f.part_time_structured_terms_match
      or f.part_time_linkedin_employment_match
      or f.part_time_xing_employment_match
      or f.part_time_title_match
    ) as part_time_high_signal
  from flags f
),
eligible as (
  select
    c.*,
    (
      c.ws_description_match
      and not c.ws_high_signal
      and not (c.internship_high_signal or c.part_time_high_signal)
    ) as ws_description_eligible,
    (
      c.internship_description_match
      and not c.internship_high_signal
      and not (c.ws_high_signal or c.part_time_high_signal)
    ) as internship_description_eligible
  from classified c
)
select
  e.platform,
  e.job_id,
  e.job_url,
  e.job_title,
  e.company_name,
  e.job_location,
  e.posted_time_ago,
  e.job_description,
  e.scraped_at,
  e.first_seen_at,
  e.last_seen_at,
  e.parse_ok,
  e.last_error,
  e.extracted_skills,
  e.posted_at_utc,
  e.posted_at_source,
  e.posted_at_parse_ok,
  e.posted_at_parse_detail,
  e.is_active,
  e.employment_type,
  e.contract_type,
  e.work_type,
  e.part_time_flag,
  (e.ws_high_signal or e.ws_description_eligible) as is_working_student,
  (e.internship_high_signal or e.internship_description_eligible) as is_internship,
  (e.part_time_high_signal or e.part_time_description_match) as is_part_time,
  (
    e.ws_high_signal
    or e.ws_description_eligible
    or e.internship_high_signal
    or e.internship_description_eligible
    or e.part_time_high_signal
    or e.part_time_description_match
  ) as is_target_role,
  array_remove(
    array[
      case
        when (
          e.ws_structured_terms_match
          or e.ws_xing_employment_match
          or e.internship_structured_terms_match
          or e.internship_linkedin_employment_match
          or e.explicit_part_time_flag_match
          or e.part_time_structured_terms_match
          or e.part_time_linkedin_employment_match
          or e.part_time_xing_employment_match
        ) then 'structured'
      end,
      case when (e.ws_title_match or e.internship_title_match or e.part_time_title_match) then 'title' end,
      case when (e.ws_description_eligible or e.internship_description_eligible or e.part_time_description_match) then 'description' end
    ]::text[],
    null
  ) as match_sources,
  array_remove(
    array[
      case when e.ws_structured_terms_match then 'working_student.structured_terms' end,
      case when e.ws_xing_employment_match then 'working_student.xing_employment_type' end,
      case when e.ws_title_match then 'working_student.title_terms' end,
      case when e.ws_description_eligible then 'working_student.description_terms' end,
      case when e.internship_structured_terms_match then 'internship.structured_terms' end,
      case when e.internship_linkedin_employment_match then 'internship.linkedin_employment_type' end,
      case when e.internship_title_match then 'internship.title_terms' end,
      case when e.internship_description_eligible then 'internship.description_terms' end,
      case when e.explicit_part_time_flag_match then 'part_time.part_time_flag' end,
      case when e.part_time_structured_terms_match then 'part_time.structured_terms' end,
      case when e.part_time_linkedin_employment_match then 'part_time.linkedin_employment_type' end,
      case when e.part_time_xing_employment_match then 'part_time.xing_employment_type' end,
      case when e.part_time_title_match then 'part_time.title_terms' end,
      case when e.part_time_description_match then 'part_time.description_terms' end
    ]::text[],
    null
  ) as match_reasons,
  case
    when not (
      e.ws_high_signal
      or e.ws_description_eligible
      or e.internship_high_signal
      or e.internship_description_eligible
      or e.part_time_high_signal
      or e.part_time_description_match
    ) then null
    when (
      e.ws_high_signal
      or e.internship_high_signal
      or e.part_time_high_signal
    ) then 'high'
    when (
      (
        e.ws_description_eligible
        and not e.contradictory_structured_match
      )
      or (
        e.internship_description_eligible
        and not e.contradictory_structured_match
      )
      or (
        e.part_time_description_match
        and not (
          e.part_time_high_signal
        )
      )
    ) then 'medium'
    else 'low'
  end as target_role_confidence,
  case
    when not (
      e.ws_high_signal
      or e.ws_description_eligible
      or e.internship_high_signal
      or e.internship_description_eligible
      or e.part_time_high_signal
      or e.part_time_description_match
    ) then null::smallint
    when (
      e.ws_high_signal
      or e.internship_high_signal
      or e.part_time_high_signal
    ) then 3::smallint
    when (
      (
        e.ws_description_eligible
        and not e.contradictory_structured_match
      )
      or (
        e.internship_description_eligible
        and not e.contradictory_structured_match
      )
      or (
        e.part_time_description_match
        and not e.part_time_high_signal
      )
    ) then 2::smallint
    else 1::smallint
  end as target_role_confidence_rank
from eligible e
;

create or replace view job_scrape.target_jobs_v as
select *
from job_scrape.jobs_target_classification_v
where is_target_role;

create materialized view if not exists job_scrape.target_jobs_m as
select *
from job_scrape.target_jobs_v
with no data;

refresh materialized view job_scrape.target_jobs_m;

create unique index if not exists idx_target_jobs_m_job
  on job_scrape.target_jobs_m (platform, job_id);
create index if not exists idx_target_jobs_m_active_confidence
  on job_scrape.target_jobs_m (is_active, target_role_confidence_rank desc);
create index if not exists idx_target_jobs_m_posted_at
  on job_scrape.target_jobs_m (posted_at_utc desc);
create index if not exists idx_target_jobs_m_first_seen
  on job_scrape.target_jobs_m (first_seen_at desc);
create index if not exists idx_target_jobs_m_categories
  on job_scrape.target_jobs_m (is_working_student, is_internship, is_part_time);
"""


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()
    print("target_job_views_ready")


if __name__ == "__main__":
    main()
