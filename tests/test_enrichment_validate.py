from __future__ import annotations

import pandas as pd
import pytest

from enrichment.validate import (
    ALLOWED_CATEGORY,
    ALLOWED_SENIORITY,
    validate_producer_vs_enrichment,
)


def _valid_enriched_like(producer: pd.DataFrame) -> pd.DataFrame:
    n = len(producer)
    return pd.DataFrame(
        {
            "Index": producer["Index"],
            "Position title": producer["Position title"],
            "complexity_score": [50] * n,
            "category": ["Engineering"] * n,
            "seniority_level": ["Mid"] * n,
        }
    )


def test_validate_passes_identical_keys():
    prod = pd.DataFrame({"Index": [1, 2], "Position title": ["A", "B"]})
    enriched = _valid_enriched_like(prod)
    validate_producer_vs_enrichment(prod, enriched)


def test_validate_empty_frames():
    prod = pd.DataFrame({"Index": pd.Series(dtype="int64"), "Position title": pd.Series(dtype="object")})
    enriched = pd.DataFrame(
        {
            "Index": pd.Series(dtype="int64"),
            "Position title": pd.Series(dtype="object"),
            "complexity_score": pd.Series(dtype="int64"),
            "category": pd.Series(dtype="object"),
            "seniority_level": pd.Series(dtype="object"),
        }
    )
    validate_producer_vs_enrichment(prod, enriched)


def test_validate_rejects_row_count_mismatch():
    prod = pd.DataFrame({"Index": [1], "Position title": ["A"]})
    bad = _valid_enriched_like(pd.DataFrame({"Index": [1, 2], "Position title": ["A", "B"]}))
    with pytest.raises(ValueError, match="row count"):
        validate_producer_vs_enrichment(prod, bad)


def test_validate_rejects_key_drift():
    prod = pd.DataFrame({"Index": [1], "Position title": ["A"]})
    enriched = _valid_enriched_like(prod)
    enriched.loc[0, "Position title"] = "B"
    with pytest.raises(ValueError, match="altered producer"):
        validate_producer_vs_enrichment(prod, enriched)


def test_validate_rejects_score_out_of_range():
    prod = pd.DataFrame({"Index": [1], "Position title": ["X"]})
    enriched = _valid_enriched_like(prod)
    enriched.loc[0, "complexity_score"] = 101
    with pytest.raises(ValueError, match="range"):
        validate_producer_vs_enrichment(prod, enriched)


def test_validate_rejects_unknown_category():
    prod = pd.DataFrame({"Index": [1], "Position title": ["X"]})
    enriched = _valid_enriched_like(prod)
    enriched.loc[0, "category"] = "Legal"
    with pytest.raises(ValueError, match="unknown category"):
        validate_producer_vs_enrichment(prod, enriched)


def test_allowed_sets_documented():
    assert "Engineering" in ALLOWED_CATEGORY
    assert "Lead" in ALLOWED_SENIORITY
