"""
Validate that enriched output aligns with producer keys and satisfies schema constraints.
"""

from __future__ import annotations

import pandas as pd

PRODUCER_KEY_COLUMNS = ("Index", "Position title")

ALLOWED_CATEGORY = frozenset({"Engineering", "Product", "Design", "Operations", "Other"})
ALLOWED_SENIORITY = frozenset({"Junior", "Mid", "Senior", "Lead"})


class EnrichmentValidationError(ValueError):
    """Raised when enriched output does not match producer keys or schema constraints."""


def validate_producer_vs_enrichment(
    producer_keys: pd.DataFrame,
    enriched: pd.DataFrame,
) -> None:
    """
    Ensure enrichment preserved every producer row and key columns verbatim (as strings).

    Args:
        producer_keys: Columns ``Index`` and ``Position title`` from the pre-enrichment frame.
        enriched: Output of ``enrich_jobs_dataframe`` (includes enrichment columns).

    Raises:
        EnrichmentValidationError: On row-count mismatch, missing columns, key drift, or invalid
            enriched fields.
    """
    for col in PRODUCER_KEY_COLUMNS:
        if col not in producer_keys.columns:
            raise EnrichmentValidationError(f"validate: producer data missing column {col!r}")
        if col not in enriched.columns:
            raise EnrichmentValidationError(f"validate: enriched data missing column {col!r}")

    n_in = len(producer_keys)
    n_out = len(enriched)
    if n_in != n_out:
        raise EnrichmentValidationError(
            f"Enrichment row count mismatch: producer has {n_in} row(s), enriched has {n_out} "
            "(producer payload and enrichment output must be 1:1)."
        )

    for col in PRODUCER_KEY_COLUMNS:
        left = producer_keys[col].astype(str).str.strip().reset_index(drop=True)
        right = enriched[col].astype(str).str.strip().reset_index(drop=True)
        if not left.equals(right):
            diff = left != right
            pos = int(diff.idxmax()) if diff.any() else -1
            raise EnrichmentValidationError(
                f"Enrichment altered producer column {col!r} at row {pos}: "
                f"producer={left.iloc[pos]!r} vs enriched={right.iloc[pos]!r}"
            )

    _validate_enrichment_columns(enriched)


def _validate_enrichment_columns(enriched: pd.DataFrame) -> None:
    for col in ("complexity_score", "category", "seniority_level"):
        if col not in enriched.columns:
            raise EnrichmentValidationError(f"validate: enriched data missing column {col!r}")

    scores = pd.to_numeric(enriched["complexity_score"], errors="coerce")
    if scores.isna().any():
        bad = enriched.loc[scores.isna()].index.tolist()
        raise EnrichmentValidationError(
            f"validate: complexity_score is null or non-numeric at rows {bad[:5]!r}"
        )

    if (scores < 0).any() or (scores > 100).any():
        raise EnrichmentValidationError(
            "validate: complexity_score out of range [0, 100]: "
            f"min={int(scores.min())}, max={int(scores.max())}"
        )

    cat = enriched["category"].astype(str)
    if cat.isna().any() or (cat.str.strip() == "").any():
        raise EnrichmentValidationError("validate: category must be non-empty string on every row")
    bad_cat = ~cat.isin(ALLOWED_CATEGORY)
    if bad_cat.any():
        row = int(bad_cat.idxmax())
        raise EnrichmentValidationError(
            f"validate: unknown category at row {row}: {enriched.loc[row, 'category']!r} "
            f"(allowed: {sorted(ALLOWED_CATEGORY)})"
        )

    sen = enriched["seniority_level"].astype(str)
    if sen.isna().any() or (sen.str.strip() == "").any():
        raise EnrichmentValidationError(
            "validate: seniority_level must be non-empty string on every row"
        )
    bad_s = ~sen.isin(ALLOWED_SENIORITY)
    if bad_s.any():
        row = int(bad_s.idxmax())
        raise EnrichmentValidationError(
            f"validate: unknown seniority_level at row {row}: {enriched.loc[row, 'seniority_level']!r} "
            f"(allowed: {sorted(ALLOWED_SENIORITY)})"
        )
