"""Profiling data types for schema analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnProfile:
    """Profile statistics for a single column."""

    column_name: str
    completeness: float
    approximate_num_distinct: int | None = None
    data_type: str | None = None
    is_data_type_inferred: bool | None = None
    type_counts: dict[str, int] | None = None
    histogram: list[dict[str, Any]] | None = None
    kll_sketch: Any | None = None
    maximum: Any | None = None
    minimum: Any | None = None
    mean: float | None = None
    sum: float | None = None
    standard_deviation: float | None = None


@dataclass
class DataFrameProfile:
    """Complete profile of a DataFrame."""

    num_records: int
    columns: dict[str, ColumnProfile]
