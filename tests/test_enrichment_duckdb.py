from __future__ import annotations

from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from enrichment.duckdb_enrich import enrich_jobs_dataframe, enrich_parquet_bytes


def _minimal_parquet_bytes(rows: list[tuple[int, str]]) -> bytes:
    idx, titles = zip(*rows, strict=True)
    buf = BytesIO()
    table = pa.table({"Index": list(idx), "Position title": list(titles)})
    pq.write_table(table, buf)
    return buf.getvalue()


def test_enrich_jobs_dataframe_columns_and_score_bounds():
    df = pd.DataFrame(
        {
            "Index": [1, 2],
            "Position title": ["Senior Backend Engineer", "Intern"],
            "combined_text": ["Senior Backend Engineer", "Intern"],
        }
    )
    out = enrich_jobs_dataframe(df)
    assert list(out.columns) == [
        "Index",
        "Position title",
        "complexity_score",
        "category",
        "seniority_level",
    ]
    assert len(out) == 2
    for score in out["complexity_score"]:
        assert 0 <= int(score) <= 100


@pytest.mark.parametrize(
    ("title", "expected_category_fragment"),
    [
        ("Backend Engineer", "Engineering"),
        ("Product Manager", "Product"),
        ("UX Designer", "Design"),
        ("HR Generalist", "Operations"),
        ("Chief Happiness Officer", "Other"),
    ],
)
def test_enrich_jobs_category(title: str, expected_category_fragment: str):
    df = pd.DataFrame(
        {
            "Index": [1],
            "Position title": [title],
            "combined_text": [title],
        }
    )
    out = enrich_jobs_dataframe(df)
    assert out.iloc[0]["category"] == expected_category_fragment


def test_enrich_parquet_bytes_roundtrip_and_schema():
    raw = _minimal_parquet_bytes([(1, "Data Engineer"), (2, "VP Engineering")])
    enriched = enrich_parquet_bytes(raw)
    assert enriched != raw
    tbl = pq.read_table(BytesIO(enriched))
    assert tbl.num_rows == 2
    names = tbl.column_names
    assert "complexity_score" in names
    assert "category" in names
    assert "seniority_level" in names


def test_enrich_parquet_bytes_idempotent():
    raw = _minimal_parquet_bytes([(1, "Foo")])
    once = enrich_parquet_bytes(raw)
    twice = enrich_parquet_bytes(once)
    assert twice == once


def test_enrich_jobs_dataframe_missing_combined_text_raises():
    df = pd.DataFrame({"Index": [1], "Position title": ["x"]})
    with pytest.raises(ValueError, match="missing columns"):
        enrich_jobs_dataframe(df)


def test_enrich_parquet_bytes_rejects_missing_columns():
    buf = BytesIO()
    pq.write_table(pa.table({"Position title": ["x"]}), buf)
    with pytest.raises(ValueError, match="Index"):
        enrich_parquet_bytes(buf.getvalue())


def test_enrich_parquet_bytes_with_job_description():
    buf = BytesIO()
    pq.write_table(
        pa.table(
            {
                "Index": [1],
                "Position title": ["Software Engineer"],
                "job_description": ["Requires Python, AWS, and 5+ years experience."],
            }
        ),
        buf,
    )
    out = enrich_parquet_bytes(buf.getvalue())
    df = pq.read_table(BytesIO(out)).to_pandas()
    score = int(df.iloc[0]["complexity_score"])
    # Explicit "5+ years", Python, AWS → experience + skill terms beyond title-only heuristics
    assert score >= 30
