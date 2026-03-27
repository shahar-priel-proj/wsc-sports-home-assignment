# DuckDB enrichment

Logic lives here so the **producer** stays raw (two Parquet columns) and enrichment can run in the **consumer** (`ENRICH_WITH_DUCKDB=1`) or in an external **stream processor**.

When enrichment runs, each row gets three extra columns:

| Column | Description |
|--------|-------------|
| `complexity_score` | Integer **0–100** (see formula below). |
| `category` | `Engineering`, `Product`, `Design`, `Operations`, or `Other`. |
| `seniority_level` | `Junior`, `Mid`, `Senior`, or `Lead` (first matching rule wins). |

## Producer vs enrichment validation

After DuckDB runs, `validate_producer_vs_enrichment` checks:

- **Same row count** as the producer keys.
- **`Index` and `Position title`** match the input **exactly** (string compare after strip), row by row.
- **`complexity_score`** is numeric, non-null, and in **0–100**.
- **`category` / `seniority_level`** are non-empty and one of the **allowed** values (see table above).

On failure, enrichment raises **`ValueError`** with a short message (consumer upload is not attempted for that message). Call `validate_producer_vs_enrichment` yourself if you enrich outside `enrich_jobs_dataframe`.

## Data source

- **Default:** `combined_text` = **Position title** only (fast).
- **Stronger signals:** if the Parquet payload includes a **`job_description`** column, that text is concatenated with the title for scoring. The producer does not emit that column today; add it in the pipeline if you need body text without extra HTTP in the consumer.

## `complexity_score` formula

Computed in DuckDB from three capped components:

1. **Leadership / seniority keywords** (0–40) — Python UDF `leadership_kw_score`: chief/VP/director/head → 40; lead/principal/tech lead → 36; senior/architect/staff → 28; junior/intern → 8; mid cues → 16; default → 14.
2. **Years of experience** (0–30) — Uses `greatest(max_years, inferred_years_if_missing) × 3`, capped at 30. `max_years` parses phrases like `5+ years` in JDs. If there are **no** such phrases (common for **title-only** text), `inferred_years_if_missing` maps seniority words to a rough year count (e.g. director → 10, lead → 7, senior → 5) so the score is not stuck at the leadership term alone.
3. **Skill tokens** (0–30) — UDF `count_skills`: distinct matches from a fixed list (languages, clouds, **backend/frontend/ML**, FinOps, etc.); contribution `min(count × 2, 30)`.

Final: `min(100, sum of the three)`.

## Category rules

Regex on **lower** `combined_text`, **first** matching branch in SQL order:

1. Engineering — engineer, developer, software, DevOps, SRE, algorithm, backend/frontend, ML, QA, security engineer, etc.
2. Product — product manager/owner, PM, director of product.
3. Design — designer, UX, UI, motion, After Effects, creative, Figma.
4. Operations — finance, FP&A, marketing, sales, HR, legal, office, partnerships, BizDev, client solutions, etc.
5. Other — no match.

## Seniority rules

Evaluated in order (first hit):

1. **Lead** — chief, VP, director, head of, principal (leadership), lead, architect, tech lead, staff engineer, etc.
2. **Senior** — senior, sr., staff (if not already Lead).
3. **Junior** — junior, jr., intern, graduate, entry level.
4. **Mid** — default.

Heuristics can misclassify edge titles; adjust patterns in `enrichment/duckdb_enrich.py` as needed.

## API

- `enrich_parquet_bytes(parquet_bytes: bytes) -> bytes` — read/write Parquet in memory; skips work if `complexity_score` is already present.
- `enrich_jobs_dataframe(df)` — enrich a pandas `DataFrame` with `Position title` (and optional `job_description`).
