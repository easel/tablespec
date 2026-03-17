"""Generate Great Expectations baseline expectations from UMF metadata.

This module provides deterministic expectation generation from UMF schema files,
with no dependencies on Spark or profiling data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import yaml

from tablespec.format_utils import convert_umf_format_to_strftime
from tablespec.models.umf import REDUNDANT_VALIDATION_TYPES
from tablespec.type_mappings import map_to_gx_spark_type

logger = logging.getLogger(__name__)

# Required baseline expectation types that must exist for every column
# Note: expect_column_to_exist was removed because it is redundant with schema metadata
REQUIRED_BASELINE_EXPECTATION_TYPES: frozenset[str] = frozenset()


class DomainTypeExpectationGenerator:
    """Generate expectations from domain type specifications.

    This generator creates validation expectations based on pre-defined
    domain type specifications in the domain_types.yaml registry.
    """

    def __init__(self) -> None:
        """Initialize domain type expectation generator."""
        self.logger = logging.getLogger(self.__class__.__name__)
        # Lazy import to avoid circular dependencies
        self._registry = None

    @property
    def registry(self):
        """Lazy load the domain type registry."""
        if self._registry is None:
            try:
                from tablespec.inference.domain_types import DomainTypeRegistry

                self._registry = DomainTypeRegistry()
            except ImportError:
                self.logger.debug("DomainTypeRegistry not available")
                self._registry = None
        return self._registry

    def generate_domain_type_expectations(self, column: dict[str, Any]) -> list[dict[str, Any]]:
        """Generate expectations based on column's domain type.

        Args:
        ----
            column: Column dictionary from UMF with potential domain_type field

        Returns:
        -------
            List of expectation dictionaries based on domain type

        """
        expectations = []

        # Check if column has a domain type
        domain_type = column.get("domain_type")
        if not domain_type:
            return expectations

        # Check if registry is available
        if self.registry is None:
            return expectations

        # Get validation specs from registry (can be multiple validations per domain)
        validation_specs = self.registry.get_validation_specs(domain_type)
        if not validation_specs:
            self.logger.debug(f"No validation specs found for domain type: {domain_type}")
            return expectations

        # Create expectation for each validation spec
        for validation_spec in validation_specs:
            expectation = {
                "type": validation_spec["type"],
                "kwargs": {"column": column["name"], **validation_spec.get("kwargs", {})},
                "meta": {
                    "description": f"Column {column['name']} must conform to {domain_type} domain type standards",
                    "severity": validation_spec.get("severity", "warning"),
                    "generated_from": "domain_type",
                    "domain_type": domain_type,
                },
            }

            expectations.append(expectation)
            self.logger.debug(
                f"Generated domain type expectation for {column['name']}: "
                + f"{validation_spec['type']} (domain: {domain_type})"
            )

        return expectations


# Backward compatibility alias
def _convert_umf_date_format_to_strftime(umf_format: str) -> str:
    """Deprecated: Use convert_umf_format_to_strftime() from format_utils instead."""
    return convert_umf_format_to_strftime(umf_format)


class BaselineExpectationGenerator:
    """Generate baseline Great Expectations rules from UMF metadata.

    This generates simple, deterministic expectations that don't require LLM reasoning:
    - Nullability (from UMF nullable field)
    - Length constraints (from UMF length/max_length)
    - Structural checks (column count, column list)
    - Date/timestamp/numeric casting validation
    - Domain type validations (from domain_types.yaml registry)
    - Profiling-based expectations (uniqueness, ranges, completeness)

    Note: Column existence and column type expectations are intentionally NOT generated
    as they are classified as REDUNDANT_VALIDATION_TYPES (covered by schema metadata).
    """

    def __init__(self) -> None:
        """Initialize the baseline expectation generator."""
        self.domain_type_generator = DomainTypeExpectationGenerator()

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

        This includes both standard baseline expectations and domain type expectations.

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

        # 1. Nullability (from UMF nullable field, not profiling)
        # Note: expect_column_to_exist and expect_column_values_to_be_of_type
        # are no longer generated here as they are in REDUNDANT_VALIDATION_TYPES
        nullable = column.get("nullable", {})
        if nullable:
            # Check if required for any context
            required_contexts = [ctx for ctx, is_null in nullable.items() if not is_null]
            if required_contexts:
                expectations.append(
                    {
                        "type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": column_name},
                        "meta": {
                            "description": f"Column {column_name} is required (nullable=false) for contexts: {', '.join(required_contexts)}",
                            "severity": "critical",
                            "contexts": required_contexts,
                            "generated_from": "baseline",
                        },
                    }
                )

        # 2. Length constraints
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

        # 3. Date format and casting validation (if DATE type)
        if data_type == "DateType" or data_type.upper() == "DATE":
            umf_format = column.get("format", "YYYY-MM-DD")
            expectations.append(
                {
                    "type": "expect_column_values_to_cast_to_type",
                    "kwargs": {
                        "column": column_name,
                        "target_type": "DATE",
                        "format": umf_format,
                        "mostly": 1.0,
                    },
                    "meta": {
                        "description": f"Column {column_name} values must successfully cast to DATE with format {umf_format} (catches invalid dates like 2023-02-30)",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )
            strict_format = column.get("format")
            if strict_format:
                expectations.append(
                    {
                        "type": "expect_column_values_to_match_strftime_format",
                        "kwargs": {
                            "column": column_name,
                            "strftime_format": convert_umf_format_to_strftime(strict_format),
                        },
                        "meta": {
                            "description": f"Column {column_name} values must match format {strict_format}",
                            "severity": "info",
                            "generated_from": "baseline",
                        },
                    }
                )
            else:
                # Legacy fallback: default date format check when no explicit format specified
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

        # 4. Timestamp casting validation (if DATETIME/TIMESTAMP type)
        if data_type in ("TimestampType", "DateTimeType") or data_type.upper() in (
            "DATETIME",
            "TIMESTAMP",
        ):
            # Read format from UMF column, default to YYYY-MM-DD HH:MM:SS if not specified
            timestamp_format = column.get("format", "YYYY-MM-DD HH:MM:SS")
            expectations.append(
                {
                    "type": "expect_column_values_to_cast_to_type",
                    "kwargs": {
                        "column": column_name,
                        "target_type": "TIMESTAMP",
                        "format": timestamp_format,
                        "mostly": 1.0,
                    },
                    "meta": {
                        "description": f"Column {column_name} values must successfully cast to TIMESTAMP with format {timestamp_format}",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )
            strict_format = column.get("format")
            if strict_format:
                expectations.append(
                    {
                        "type": "expect_column_values_to_match_strftime_format",
                        "kwargs": {
                            "column": column_name,
                            "strftime_format": convert_umf_format_to_strftime(strict_format),
                        },
                        "meta": {
                            "description": f"Column {column_name} values must match format {strict_format}",
                            "severity": "info",
                            "generated_from": "baseline",
                        },
                    }
                )

        # 5. Integer casting validation (if INTEGER type)
        if data_type == "IntegerType" or data_type.upper() == "INTEGER":
            expectations.append(
                {
                    "type": "expect_column_values_to_cast_to_type",
                    "kwargs": {"column": column_name, "target_type": "INTEGER", "mostly": 1.0},
                    "meta": {
                        "description": f"Column {column_name} values must successfully cast to INTEGER (no letters or decimals)",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )

        # 6. Numeric casting validation (if FLOAT/DOUBLE/DECIMAL type)
        if data_type in ("FloatType", "DoubleType", "DecimalType") or data_type.upper() in (
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
        ):
            expectations.append(
                {
                    "type": "expect_column_values_to_cast_to_type",
                    "kwargs": {"column": column_name, "target_type": "DOUBLE", "mostly": 1.0},
                    "meta": {
                        "description": f"Column {column_name} values must successfully cast to numeric type",
                        "severity": "critical",
                        "generated_from": "baseline",
                    },
                }
            )

        # 7. Domain type expectations (if applicable)
        domain_expectations = self.domain_type_generator.generate_domain_type_expectations(column)
        expectations.extend(domain_expectations)

        # 8. Profiling-based expectations (if profiling data attached to column)
        profiling_expectations = self._generate_profiling_expectations(column)
        expectations.extend(profiling_expectations)

        return expectations

    def _generate_profiling_expectations(
        self, column: dict[str, Any], strictness: str = "medium"
    ) -> list[dict[str, Any]]:
        """Generate expectations from profiling data attached to UMF column.

        Args:
        ----
            column: Column dictionary from UMF with optional profiling data
            strictness: Strictness level (reserved for future use)

        Returns:
        -------
            List of expectation dictionaries derived from profiling statistics

        """
        expectations: list[dict[str, Any]] = []
        profiling = column.get("profiling", {})
        if not profiling:
            return expectations

        col_name = column["name"]

        # Uniqueness from high cardinality
        num_distinct = profiling.get("approximate_num_distinct")
        num_records = profiling.get("num_records")
        if num_distinct and num_records and num_distinct == num_records:
            expectations.append(
                {
                    "type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": col_name},
                    "meta": {
                        "description": f"Column {col_name} appears unique based on profiling",
                        "severity": "warning",
                        "generated_from": "profiling",
                    },
                }
            )

        # Range from min/max
        minimum = profiling.get("minimum")
        maximum = profiling.get("maximum")
        if minimum is not None and maximum is not None:
            expectations.append(
                {
                    "type": "expect_column_values_to_be_between",
                    "kwargs": {"column": col_name, "min_value": minimum, "max_value": maximum},
                    "meta": {
                        "description": f"Column {col_name} values between {minimum} and {maximum} based on profiling",
                        "severity": "warning",
                        "generated_from": "profiling",
                    },
                }
            )

        # Not-null from high completeness
        completeness = profiling.get("completeness")
        if completeness is not None and completeness > 0.99:
            expectations.append(
                {
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col_name},
                    "meta": {
                        "description": f"Column {col_name} has {completeness:.1%} completeness in profiling",
                        "severity": "warning",
                        "generated_from": "profiling",
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
