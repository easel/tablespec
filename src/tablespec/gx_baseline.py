"""Generate Great Expectations baseline expectations from UMF metadata.

This module provides deterministic expectation generation from UMF schema files,
with no dependencies on Spark or profiling data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import yaml

from tablespec.type_mappings import map_to_gx_spark_type

logger = logging.getLogger(__name__)


class BaselineExpectationGenerator:
    """Generate baseline Great Expectations rules from UMF metadata.

    This generates simple, deterministic expectations that don't require LLM reasoning:
    - Column existence
    - Column types
    - Nullability (from UMF nullable field)
    - Length constraints (from UMF length/max_length)
    - Structural checks (column count, column list)
    """

    def generate_baseline_expectations(
        self, umf_data: dict[str, Any], include_structural: bool = True
    ) -> list[dict[str, Any]]:
        """Generate baseline expectations from UMF metadata.

        Args:
        ----
            umf_data: UMF dictionary (loaded from YAML)
            include_structural: Include table-level structural checks

        Returns:
        -------
            List of expectation dictionaries

        """
        expectations = []

        # Structural checks (table-level)
        if include_structural:
            expectations.extend(self._generate_structural_expectations(umf_data))

        # Column-level baseline expectations
        for column in umf_data.get("columns", []):
            expectations.extend(self.generate_baseline_column_expectations(column))

        return expectations

    def _generate_structural_expectations(
        self, umf_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Generate table-level structural expectations.

        Args:
        ----
            umf_data: UMF dictionary

        Returns:
        -------
            List of structural expectation dictionaries

        """
        expectations = []
        columns = umf_data.get("columns", [])
        column_names = [col["name"] for col in columns]

        # Expect specific column count
        if columns:
            expectations.append(
                {
                    "type": "expect_table_column_count_to_equal",
                    "kwargs": {"value": len(columns)},
                    "meta": {
                        "description": f"Table must have exactly {len(columns)} columns",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )

            # Expect columns to match ordered list
            expectations.append(
                {
                    "type": "expect_table_columns_to_match_ordered_list",
                    "kwargs": {"column_list": column_names},
                    "meta": {
                        "description": "Table columns must match expected schema in order",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )

        return expectations

    def generate_baseline_column_expectations(
        self, column: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Generate baseline expectations for a single column from UMF metadata.

        Args:
        ----
            column: Column dictionary from UMF

        Returns:
        -------
            List of expectation dictionaries

        """
        expectations = []
        column_name = column["name"]
        data_type = column.get("data_type", "STRING")
        gx_type = map_to_gx_spark_type(data_type)

        # 1. Column existence
        expectations.append(
            {
                "type": "expect_column_to_exist",
                "kwargs": {"column": column_name},
                "meta": {
                    "description": f"Column {column_name} must exist in table schema",
                    "severity": "critical",
                    "generated_from": "baseline",
                },
            }
        )

        # 2. Column type
        expectations.append(
            {
                "type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": column_name, "type_": gx_type},
                "meta": {
                    "description": f"Column {column_name} must be {gx_type} (from UMF: {data_type})",
                    "severity": "info",
                    "generated_from": "baseline",
                },
            }
        )

        # 3. Nullability (from UMF nullable field, not profiling)
        nullable = column.get("nullable", {})
        if nullable:
            # Check if required for any LOB
            required_lobs = [lob for lob, is_null in nullable.items() if not is_null]
            if required_lobs:
                expectations.append(
                    {
                        "type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": column_name},
                        "meta": {
                            "description": f"Column {column_name} is required (nullable=false) for LOBs: {', '.join(required_lobs)}",
                            "severity": "critical",
                            "lob": required_lobs,
                            "generated_from": "baseline",
                        },
                    }
                )

        # 4. Length constraints
        max_length = column.get("max_length") or column.get("length")
        if max_length:
            expectations.append(
                {
                    "type": "expect_column_value_lengths_to_be_between",
                    "kwargs": {"column": column_name, "max_value": max_length},
                    "meta": {
                        "description": f"Column {column_name} values must not exceed {max_length} characters (from UMF max_length/length)",
                        "severity": "warning",
                        "generated_from": "baseline",
                    },
                }
            )

        # 5. Date format (if DATE type)
        if data_type.upper() == "DATE":
            expectations.append(
                {
                    "type": "expect_column_values_to_match_strftime_format",
                    "kwargs": {"column": column_name, "strftime_format": "%Y%m%d"},
                    "meta": {
                        "description": f"Column {column_name} must match YYYYMMDD date format (standard for DATE type)",
                        "severity": "warning",
                        "generated_from": "baseline",
                    },
                }
            )

        return expectations


class UmfToGxMapper:
    """Maps UMF with optional profiling data to Great Expectations expectation suite.

    This class provides full Great Expectations suite generation including:
    - Baseline expectations from UMF metadata
    - Profiling-based expectations (if profiling data is present)
    - Complete suite structure with metadata
    """

    def __init__(self) -> None:
        """Initialize the UMF to GX mapper."""
        self.baseline_generator = BaselineExpectationGenerator()

    def generate_expectations(
        self,
        umf_file: Path | str,
        strictness: Literal["loose", "medium", "strict"] = "medium",
    ) -> dict[str, Any]:
        """Generate GX expectation suite from UMF with profiling data.

        Args:
        ----
            umf_file: Path to UMF YAML file
            strictness: Strictness level (loose, medium, strict)

        Returns:
        -------
            Dictionary representing GX expectation suite

        """
        # Load UMF
        umf_path = Path(umf_file)
        with umf_path.open(encoding="utf-8") as f:
            umf = yaml.safe_load(f)

        table_name = umf.get("table_name", "unknown")
        logger.info(
            f"Generating expectations for {table_name} with strictness={strictness}"
        )

        # Build expectation suite
        suite = {
            "name": f"{table_name}_suite",
            "meta": {
                "table_name": table_name,
                "generated_by": "tablespec",
                "strictness": strictness,
                "source_umf": str(umf_path),
            },
            "expectations": [],
        }

        # Generate baseline expectations from UMF metadata
        suite["expectations"].extend(
            self.baseline_generator.generate_baseline_expectations(umf)
        )

        # Generate expectations from profiling data
        for column in umf.get("columns", []):
            expectations = self._generate_profiling_expectations(column, strictness)
            suite["expectations"].extend(expectations)

        logger.info(f"Generated {len(suite['expectations'])} expectations")
        return suite

    def _generate_profiling_expectations(
        self,
        column: dict[str, Any],
        strictness: str,  # noqa: ARG002
    ) -> list[dict[str, Any]]:
        """Generate expectations from profiling data (not baseline).

        Args:
        ----
            column: Column dictionary from UMF
            strictness: Strictness level (reserved for future use)

        Returns:
        -------
            List of expectation dictionaries

        """
        expectations = []
        profiling = column.get("profiling", {})

        # Only generate profiling-based expectations if profiling data exists
        if not profiling:
            return expectations

        # TODO(dev): Add profiling-based expectations
        # - uniqueness → expect_column_values_to_be_unique
        # - min/max → expect_column_min/max_to_be_between
        # - value sets → expect_column_values_to_be_in_set
        # - patterns → expect_column_values_to_match_regex

        return expectations
