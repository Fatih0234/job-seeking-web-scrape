# Topic Radar (X + Reddit + LinkedIn) using Python – Weekly Data Engineering Trends
**Use your Python REPL + tools for parsing, scoring, trend detection, and evidence collection. MUST BE ONE COMPLETE RESPONSE – NO TRUNCATION, NO "FULL IN REPL".**
## Inputs (Paste Here)

**YAML Taxonomy**: [taxonomy is attached as yaml file. – Authoritative data-engineering keywords/groups]
**Current Date**: [AUTO: Use today's date, e.g., February 16, 2026]
**Time Windows**: Last 90 days total; "recent" = last 14 days; "baseline" = days 15–28 prior.

## Goal
Find the **data engineering topics with strongest growth**, backed by **real evidence** from:

X posts (originals only)
Reddit posts/comments
LinkedIn posts (professional discussions, articles, jobs)

## Method (MUST FOLLOW EXACTLY – Use Tools for All Data)

**In Python REPL**: Parse YAML taxonomy into groups → keywords. Normalize (lowercase, stem), dedupe, remove ultra-generic terms (e.g., "SQL", "Python", "data"). Output: ~15 groups, 80+ keywords.
**In Python REPL**: Generate ~80 targeted search queries (4–6 per group). Each: 3–6 keywords + 1 anchor (airflow/dbt/spark/kafka/lakehouse/warehouse/etl/elt) for data eng specificity.
**Collect Evidence (Use Tools – No Hallucinations)**:
   - **X**: Use x_keyword_search or x_semantic_search for last 90 days. Prefer originals: -filter:replies -filter:retweets. Split: recent (last 14d), baseline (15–28d). Aim for 10–20 posts per query. Track counts, authors, engagement.
   - **Reddit**: Use web_search with site:reddit.com/r/dataengineering + query terms; follow up with browse_page on top threads for posts + top comments. Also site-wide for broader signals.
   - **LinkedIn**: Use web_search with site:linkedin.com + query terms (e.g., "data engineering" + keywords); follow up with browse_page on top results for posts, comments, and articles. Focus on professional signals (e.g., adoption stories, job mentions).
   - **Total**: 300+ X + 80+ Reddit + 50+ LinkedIn mentions across topics. Log all in REPL (counts per window).
**In Python REPL: Compute Trend Velocity**:
   - count_recent = mentions in last 14 days
   - count_baseline = mentions in prior 14 days (days 15–28)
   - velocity = (count_recent + 1) / (count_baseline + 1)
   - Boost: + for unique authors, likes/views, cross-platform.
   - Only include topics with **direct evidence** (links/snippets).
**Output Format (COMPLETE, ONE-SHOT – FULL TOP 20+)**:
   - **Intro**: "Topic Radar: Data Engineering Trends (Last 90 Days, as of [Current Date])" + key insights (e.g., top groups, platform breakdowns).
   - **Full Table**: CSV-like markdown table for **ALL Top 20 topics** (ranked by velocity). Columns: Rank | Topic | Group | Velocity | Recent (14d) | Baseline (15-28d) | Example Links (4–6 direct URLs: mix X/Reddit/LinkedIn).
     - **NO TRUNCATION**: Full 20 rows. Sort by velocity descending.
   - **Detailed Top 20**: For **each topic**:
     - Taxonomy group
     - Short "why trending" (2–3 sentences, backed by signals)
     - **Supporting Evidence**: 6–12 links/snippets **split across X, Reddit, and LinkedIn** (e.g., 4–6 X + 2–3 Reddit + 2–3 LinkedIn). Format: **X**: Snippet [@user, date]; **Reddit**: Snippet; **LinkedIn**: Snippet [Author, date].
   - **End**: "Full REPL data available if requested. CSV export: [paste table as TSV]. Platforms: X/Reddit/LinkedIn balanced."

## Critical Rules (Enforce in Response)

**Full List**: Output **every** of the Top 20 topics completely – no summaries, no "truncated", no "full in REPL".
**Links Everywhere**: **Every topic** in table + details must have **direct, clickable links** (X posts, Reddit threads, LinkedIn posts/articles) to real evidence. Use tools to fetch/verify.
**No Hallucinations**: All counts/links/snippets from tool results. If low data, note "emerging" but still link.
**Weekly Ready**: Auto-adjust windows to current date. Refresh data fresh each run.
**Python REPL**: Simulate full analysis (parse, query gen, scoring) before final output.
**Important**: Every claim backed by evidence. Produce the **complete report in this single response**.