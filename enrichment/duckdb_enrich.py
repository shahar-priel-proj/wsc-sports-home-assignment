from __future__ import annotations

import re
from io import BytesIO

import duckdb
import pandas as pd

from enrichment.validate import validate_producer_vs_enrichment

# Unique skill / tech tokens (case-insensitive); counted once each per row text.
# Includes common *title* terms (backend, ml, finops) so title-only payloads still get skill signal.
_SKILLS_RE = re.compile(
    r"\b(?:"
    r"python|java|javascript|typescript|c\+\+|c#|\.net|golang|go\b|rust|kotlin|swift|ruby|php|scala|"
    r"react|angular|vue\.?js|node\.?js|django|flask|fastapi|spring|kafka|spark|hadoop|airflow|dbt|"
    r"tensorflow|pytorch|keras|scikit|nlp|machine learning|deep learning|computer vision|genai|llm|"
    r"aws|azure|gcp|\bml\b|\bai\b|google cloud|\bcloud\b|kubernetes|k8s|docker|terraform|ansible|jenkins|"
    r"ci/cd|git\b|github|"
    r"sql|nosql|mongodb|postgres|postgresql|mysql|redis|elasticsearch|snowflake|bigquery|databricks|"
    r"linux|unix|bash|powershell|agile|scrum|jira|graphql|rest api|microservices|devops|\bsre\b|finops|"
    r"backend|frontend|full[- ]?stack|mobile|\bios\b|android|data engineer|analytics|algorithms?|"
    r"graphics|multimodal|blockchain|saas|tableau|opencv|cuda|ffmpeg|html|css|webpack|next\.?js|express|"
    r"nginx|oauth|ssl|tls|\bui\b|\bux\b|\bqa\b|quality assurance"
    r")\b",
    re.IGNORECASE,
)

_YEARS_PATTERNS = [
    re.compile(r"(?:minimum|min\.?|at least|\+)\s*(\d{1,2})\s*\+?\s*(?:years?|yrs?)", re.I),
    re.compile(r"(\d{1,2})\s*\+\s*years?", re.I),
    re.compile(r"(\d{1,2})\s*-\s*(\d{1,2})\s*years?", re.I),
    re.compile(r"(\d{1,2})\s+years?\s+(?:of|experience|in)\b", re.I),
    re.compile(r"(\d{1,2})\s+years?\b", re.I),
]


def leadership_keyword_score(text: str) -> int:
    """
    Map leadership / seniority signals in text to a 0–40 score (part of complexity).

    Higher tiers stack toward more demanding roles.
    """
    if not text:
        return 10
    t = text.lower()
    if re.search(
        r"\b(chief|vice president|\bvp\b|director of|c-level|cxo|head of engineering|head of product)\b",
        t,
    ):
        return 40
    if re.search(
        r"\b(principal|distinguished|fellow|engineering manager|tech lead|technical lead|team lead|"
        r"lead\b|staff engineer)\b",
        t,
    ):
        return 36
    if re.search(r"\b(architect|senior|\bsr\.|\bsr\b|staff)\b", t):
        return 28
    if re.search(r"\b(junior|\bjr\.|\bintern|graduate|entry.level|entry level)\b", t):
        return 8
    if re.search(r"\b(mid|intermediate|ii\b|2\b)\b", t):
        return 16
    return 14


def max_years_in_text(text: str) -> int:
    """Largest year count implied by common JD phrases (capped at 25 for scoring)."""
    if not text:
        return 0
    best = 0
    for pat in _YEARS_PATTERNS:
        for m in pat.finditer(text):
            for g in m.groups():
                if g is not None:
                    try:
                        best = max(best, int(g))
                    except ValueError:
                        pass
    return min(best, 25)


def inferred_years_when_none_explicit(text: str) -> int:
    """
    When the text has no ``N years`` / ``N+ years`` phrases (typical for titles), estimate a
    rough experience level from seniority wording so the years term is not always zero.

    Returns 0–10 aligned with ``max_years_in_text`` scale before the ``* 3`` score multiplier.
    """
    if not text or max_years_in_text(text) > 0:
        return 0
    t = text.lower()
    if re.search(
        r"\b(chief|vice president|\bvp\b|director of|c-level|cxo|head of|distinguished|fellow)\b",
        t,
    ):
        return 10
    if re.search(
        r"\b(principal|engineering manager|tech lead|technical lead|team lead|staff engineer|architect)\b",
        t,
    ):
        return 8
    if re.search(r"\blead\b", t):
        return 7
    if re.search(r"\b(senior|\bsr\.|\bsr\b|staff)\b", t):
        return 5
    if re.search(r"\b(mid|intermediate|\bii\b)\b", t):
        return 3
    if re.search(r"\b(junior|\bjr\.|\bintern|graduate|entry.level|entry level)\b", t):
        return 1
    return 2


def count_unique_skills(text: str) -> int:
    """Count distinct skill tokens matched in text."""
    if not text:
        return 0
    found = {m.group(0).lower() for m in _SKILLS_RE.finditer(text)}
    return len(found)


def _register_udfs(con: duckdb.DuckDBPyConnection) -> None:
    con.create_function("leadership_kw_score", leadership_keyword_score, [str], int)
    con.create_function("max_years", max_years_in_text, [str], int)
    con.create_function("inferred_years_if_missing", inferred_years_when_none_explicit, [str], int)
    con.create_function("count_skills", count_unique_skills, [str], int)


def enrich_jobs_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``complexity_score``, ``category``, ``seniority_level`` using DuckDB + UDFs.

    Args:
        df: Must include ``combined_text`` and ``Position title``, ``Index``.

    Returns:
        DataFrame with ``Index``, ``Position title``, ``complexity_score``, ``category``, ``seniority_level``.
    """
    required = {"Index", "Position title", "combined_text"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"enrich_jobs_dataframe: missing columns {missing}")

    con = duckdb.connect(database=":memory:")
    _register_udfs(con)
    con.register("src", df)

    query = """
    SELECT
        s."Index",
        s."Position title",
        CAST(
            least(
                100,
                leadership_kw_score(s.combined_text)
                + least(
                    greatest(
                        max_years(s.combined_text),
                        inferred_years_if_missing(s.combined_text)
                    ) * 3,
                    30
                )
                + least(count_skills(s.combined_text) * 2, 30)
            ) AS INTEGER
        ) AS complexity_score,
        CASE
            WHEN regexp_matches(lower(s.combined_text),
                'engineer|developer|software|devops|\\bsre\\b|algorithm|backend|frontend|full.?stack|'
                'data engineer|\\bml\\b|machine learning|graphics|\\bc\\+\\+|programming|'
                'cloud finops|finops|infrastructure|platform|qa |quality assurance|security engineer|'
                'nlp|algorithm developer|genai')
            THEN 'Engineering'
            WHEN regexp_matches(lower(s.combined_text),
                'product manager|product owner|\\bpm\\b|director of product|product lead')
            THEN 'Product'
            WHEN regexp_matches(lower(s.combined_text),
                'designer|ux\\b|ui\\b|motion|after effects|creative|figma|visual design')
            THEN 'Design'
            WHEN regexp_matches(lower(s.combined_text),
                'accountant|controller|finance|\\bfp&a\\b|financial|marketing|sales|'
                'legal counsel|\\bhr\\b|human resources|people&culture|people and culture|'
                'office manager|partnership|revenue|recruit|talent|operations manager|'
                'bizdev|business development|client solutions|delivery')
            THEN 'Operations'
            ELSE 'Other'
        END AS category,
        CASE
            WHEN regexp_matches(lower(s.combined_text),
                '\\bchief\\b|vice president|\\bvp\\b|\\bcdo\\b|\\bcto\\b|\\bcfo\\b|'
                'director of|head of|distinguished|fellow')
            THEN 'Lead'
            WHEN regexp_matches(lower(s.combined_text),
                '\\blead\\b|principal|staff engineer|engineering lead|tech lead|technical lead|architect')
            THEN 'Lead'
            WHEN regexp_matches(lower(s.combined_text), 'senior|\\bsr\\.?\\b|staff\\b')
            THEN 'Senior'
            WHEN regexp_matches(lower(s.combined_text),
                'junior|\\bjr\\.?\\b|intern|graduate|entry.level|entry level')
            THEN 'Junior'
            ELSE 'Mid'
        END AS seniority_level
    FROM src s
    """
    out = con.execute(query).fetchdf()
    con.close()
    validate_producer_vs_enrichment(df[["Index", "Position title"]], out)
    return out


def enrich_parquet_bytes(parquet_bytes: bytes) -> bytes:
    """
    Read producer Parquet (``Index``, ``Position title`` [+ optional ``job_description``]),
    run enrichment, return Parquet with five columns.

    If ``complexity_score`` is already present, returns input unchanged (idempotent).

    Args:
        parquet_bytes: Raw Parquet file bytes from the producer.

    Returns:
        Enriched Parquet bytes.
    """
    bio = BytesIO(parquet_bytes)
    df = pd.read_parquet(bio)
    if "complexity_score" in df.columns:
        return parquet_bytes

    required = {"Index", "Position title"}
    if not required.issubset(df.columns):
        raise ValueError(
            "enrich_parquet_bytes: Parquet must contain columns Index and Position title; "
            f"got {list(df.columns)}"
        )

    if "job_description" in df.columns:
        combined = (
            df["Position title"].astype(str).str.strip()
            + "\n"
            + df["job_description"].astype(str).str.strip()
        )
    else:
        combined = df["Position title"].astype(str).str.strip()

    work = df[["Index", "Position title"]].copy()
    work["combined_text"] = combined
    enriched = enrich_jobs_dataframe(work)
    out = BytesIO()
    enriched.to_parquet(out, index=False, engine="pyarrow")
    return out.getvalue()
