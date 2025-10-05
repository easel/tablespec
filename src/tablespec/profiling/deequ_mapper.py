"""Map PyDeequ profiling results to UMF profiling section."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tablespec.profiling.types import ColumnProfile, DataFrameProfile

logger = logging.getLogger(__name__)


class DeequToUmfMapper:
    """Maps PyDeequ profiling results to UMF profiling sections."""

    def enrich_umf_with_profiling(
        self,
        umf: dict[str, Any],
        profile: DataFrameProfile,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        """Add profiling sections to UMF columns.

        Args:
        ----
            umf: Base UMF dictionary
            profile: DataFrameProfile from PyDeequ
            sample_size: Sample size used for profiling

        Returns:
        -------
            Enriched UMF dictionary with profiling sections

        """
        # Add table-level profiling metadata
        umf["profiling_metadata"] = {
            "profiled_at": datetime.now(UTC).isoformat(),
            "tool": "pulseflow-profiler",
            "version": "1.0.0",
            "sample_size": sample_size,
            "total_rows": profile.num_records,
        }

        # Enrich each column with profiling data
        for column in umf.get("columns", []):
            column_name = column["name"]
            if column_name in profile.columns:
                column_profile = profile.columns[column_name]
                column["profiling"] = self._build_profiling_section(column_profile)

                # Override nullable based on completeness
                if column_profile.completeness < 1.0:
                    column["nullable"] = True
                    logger.debug(
                        f"Column {column_name}: Set nullable=True "
                        f"(completeness={column_profile.completeness:.2%})"
                    )

        logger.info(
            f"Enriched UMF with profiling data for {len(profile.columns)} columns"
        )
        return umf

    def _build_profiling_section(self, profile: ColumnProfile) -> dict[str, Any]:
        """Build profiling section for a single column.

        Args:
        ----
            profile: ColumnProfile from PyDeequ

        Returns:
        -------
            Dictionary representing profiling section

        """
        profiling: dict[str, Any] = {
            "completeness": profile.completeness,
        }

        # Add optional fields if available
        if profile.approximate_num_distinct is not None:
            profiling["approximate_num_distinct"] = profile.approximate_num_distinct

        if profile.data_type:
            profiling["data_type_inferred"] = profile.data_type

        # Add statistics sub-section if numeric data available
        statistics: dict[str, Any] = {}

        if profile.minimum is not None:
            statistics["min"] = profile.minimum

        if profile.maximum is not None:
            statistics["max"] = profile.maximum

        if profile.mean is not None:
            statistics["mean"] = round(profile.mean, 4)

        if profile.standard_deviation is not None:
            statistics["stddev"] = round(profile.standard_deviation, 4)

        if statistics:
            profiling["statistics"] = statistics

        return profiling
