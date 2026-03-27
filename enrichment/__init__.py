"""DuckDB-based job enrichment (consumer / stream-processor friendly)."""

from enrichment.duckdb_enrich import enrich_jobs_dataframe, enrich_parquet_bytes
from enrichment.validate import EnrichmentValidationError, validate_producer_vs_enrichment

__all__ = [
    "EnrichmentValidationError",
    "enrich_jobs_dataframe",
    "enrich_parquet_bytes",
    "validate_producer_vs_enrichment",
]
