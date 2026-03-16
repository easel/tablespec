"""Custom Great Expectations for Spark-specific validation.

This module provides custom GX expectations that validate actual Spark casting
behavior rather than just pattern matching. These expectations catch edge cases
like "2023-02-30" (valid format, invalid date) that pass regex validation but
fail when Spark attempts to cast them.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from typing import Protocol

    class ExpectationConfiguration(Protocol):  # type: ignore[misc]
        """Protocol for ExpectationConfiguration when GX unavailable."""

        kwargs: dict[str, Any]


try:
    from great_expectations.expectations.expectation import Expectation

    _gx_available = True
except ImportError:
    _gx_available = False
    # Create dummy class for type hints when GX not available
    Expectation = object  # type: ignore[misc, assignment]

GX_AVAILABLE = _gx_available

try:
    from pyspark.sql import functions as F

    _spark_available = True
except ImportError:
    _spark_available = False

SPARK_AVAILABLE = _spark_available


logger = logging.getLogger(__name__)


# Great Expectations Expectation Classes
if GX_AVAILABLE:

    class ExpectColumnValuesToCastToType(Expectation):  # type: ignore[misc]
        """Expect column values to successfully cast to a specified type.

        This expectation validates that values can be cast to the target type
        without becoming NULL, catching edge cases like invalid dates.

        This is a Spark-specific custom expectation that validates actual casting
        behavior rather than just pattern matching.
        """

        expectation_type = "expect_column_values_to_cast_to_type"

        # These define the parameters this expectation accepts
        success_keys = (
            "column",
            "target_type",
            "format",  # Optional UMF format string for DATE/TIMESTAMP
            "fallback_formats",  # Optional list of alternative formats to try
            "mostly",
        )

        # Default values for parameters
        default_kwarg_values: ClassVar[dict[str, Any]] = {
            "format": None,  # Optional - if None, uses Spark default casting
            "fallback_formats": None,  # Optional - list of fallback formats for mixed data
            "mostly": 1.0,
            "result_format": "BASIC",
        }

        # Configure Pydantic to allow our custom fields
        class Config:
            extra = "allow"  # Allow additional fields beyond the base Expectation model

        def validate_configuration(
            self, configuration: ExpectationConfiguration | None = None
        ) -> None:
            """Validate that configuration is correct."""
            super().validate_configuration(configuration)  # type: ignore[arg-type]  # GX base class accepts None but type stub is incomplete

            if configuration:
                target_type = configuration.kwargs.get("target_type")
                if target_type:
                    valid_types = [
                        "DATE",
                        "INTEGER",
                        "DOUBLE",
                        "FLOAT",
                        "TIMESTAMP",
                        "BOOLEAN",
                        "DECIMAL",
                    ]
                    if target_type.upper() not in valid_types:
                        msg = f"target_type must be one of {valid_types}, got {target_type}"
                        raise ValueError(msg)

        def _validate(
            self,
            metrics: dict,
            runtime_configuration: dict | None = None,
            execution_engine: Any = None,
        ) -> dict:
            """Validate expectation against actual data.

            This method is called by GX during actual validation.
            Performs Spark-specific casting validation.
            """
            if not SPARK_AVAILABLE:
                msg = "PySpark is required for custom casting expectations"
                raise ImportError(msg)

            # Extract parameters from self.configuration (set by GX)
            column = self.column  # type: ignore[attr-defined]
            target_type = self.target_type  # type: ignore[attr-defined]
            format_str = getattr(self, "format", None)  # type: ignore[attr-defined]
            fallback_formats = getattr(self, "fallback_formats", None)  # type: ignore[attr-defined]
            mostly = getattr(self, "mostly", 1.0)  # type: ignore[attr-defined]

            # Get Spark DataFrame from execution engine
            # In GX 1.x with Spark, the batch contains the DataFrame
            try:
                from tablespec.casting_utils import (
                    build_flexible_formats,
                    cast_column_with_format,
                    try_parse_flexible_timestamp,
                )

                # Access DataFrame through execution engine's active batch
                df = execution_engine.batch_manager.active_batch.data.dataframe

                # Check if column is already the target type (e.g., Gold tables with pre-typed columns)
                # This avoids trying to parse strings when the column is already DATE/TIMESTAMP
                from pyspark.sql.types import DateType, TimestampType

                col_schema = df.schema[column]
                col_type = col_schema.dataType
                is_already_target_type = (
                    target_type.upper() == "DATE" and isinstance(col_type, DateType)
                ) or (target_type.upper() == "TIMESTAMP" and isinstance(col_type, TimestampType))

                if is_already_target_type:
                    # Column is already the target type - validation passes
                    total_count = df.count()
                    return {
                        "success": True,
                        "result": {
                            "element_count": total_count,
                            "unexpected_count": 0,
                            "unexpected_percent": 0.0,
                            "partial_unexpected_list": [],
                            "observed_value": f"Column already typed as {col_type}",
                        },
                    }

                # Count total non-null values before casting
                original_non_null_count = df.filter(F.col(column).isNotNull()).count()  # type: ignore[attr-defined]

                if original_non_null_count == 0:
                    # Column is entirely NULL - nothing to cast
                    return {
                        "success": True,
                        "result": {
                            "element_count": 0,
                            "unexpected_count": 0,
                            "unexpected_percent": 0.0,
                            "partial_unexpected_list": [],
                            "observed_value": "Column is entirely NULL",
                        },
                    }

                # Use flexible parsing for DATE/TIMESTAMP columns
                if target_type.upper() in ("DATE", "TIMESTAMP"):
                    formats = build_flexible_formats(target_type, format_str, fallback_formats)
                    cast_expr = try_parse_flexible_timestamp(
                        F.col(column),  # type: ignore[attr-defined]
                        primary_format=formats[0] if formats else "",
                        fallback_formats=formats[1:] if len(formats) > 1 else None,
                    )
                    if target_type.upper() == "DATE":
                        cast_expr = cast_expr.cast("date")
                else:
                    # Use shared casting utility (always uses try_to_timestamp for graceful handling)
                    cast_expr = cast_column_with_format(
                        F.col(column),
                        target_type,
                        format_str,
                    )
                casted_df = df.withColumn(f"_casted_{column}", cast_expr)

                # Count nulls after casting (excluding rows that were already null)
                casting_failures_df = casted_df.filter(
                    F.col(column).isNotNull() & F.col(f"_casted_{column}").isNull()  # type: ignore[attr-defined]
                )

                unexpected_count = casting_failures_df.count()
                unexpected_percent = (
                    (unexpected_count / original_non_null_count * 100)
                    if original_non_null_count > 0
                    else 0.0
                )

                # Collect sample of values that failed casting (limited to 20 examples)
                unexpected_values = []
                if unexpected_count > 0:
                    sample_rows = casting_failures_df.select(column).limit(20).collect()
                    unexpected_values = [row[column] for row in sample_rows]

                # Calculate success
                success_percent = (
                    1.0 - (unexpected_count / original_non_null_count)
                    if original_non_null_count > 0
                    else 1.0
                )
                success = success_percent >= mostly

                format_msg = f" with format {format_str}" if format_str else ""
                if fallback_formats:
                    format_msg += f" (fallbacks: {fallback_formats})"
                return {
                    "success": success,
                    "result": {
                        "element_count": original_non_null_count,
                        "unexpected_count": unexpected_count,
                        "unexpected_percent": unexpected_percent,
                        "partial_unexpected_list": unexpected_values[
                            :10
                        ],  # Limit to 10 for reporting
                        "observed_value": f"{success_percent * 100:.2f}% cast successfully to {target_type}{format_msg}",
                    },
                }

            except Exception as e:
                logger.exception(f"Failed to execute casting validation: {e}")
                return {
                    "success": False,
                    "result": {
                        "element_count": 0,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "observed_value": f"Validation failed: {e!s}",
                    },
                }

    class ExpectColumnDateToBeInCurrentYear(Expectation):  # type: ignore[misc]
        """Expect date column values to fall within the current calendar year.

        This expectation validates that date values are between January 1st and
        December 31st of the current year. Useful for validating gap closure dates,
        transaction dates, or other fields that should only contain current-year data.

        Uses Spark SQL to dynamically compute year bounds at validation time.
        """

        expectation_type = "expect_column_date_to_be_in_current_year"

        # Parameters this expectation accepts
        success_keys = (
            "column",
            "mostly",
        )

        # Default values for parameters
        default_kwarg_values: ClassVar[dict[str, Any]] = {
            "mostly": 1.0,
            "result_format": "BASIC",
        }

        # Configure Pydantic to allow our custom fields
        class Config:
            extra = "allow"

        def _validate(
            self,
            metrics: dict,
            runtime_configuration: dict | None = None,
            execution_engine: Any = None,
        ) -> dict:
            """Validate that date values fall within the current calendar year.

            This method is called by GX during actual validation.
            Uses Spark SQL to compute dynamic year bounds.
            """
            if not SPARK_AVAILABLE:
                msg = "PySpark is required for current year date validation"
                raise ImportError(msg)

            # Extract parameters
            column = self.column  # type: ignore[attr-defined]
            mostly = getattr(self, "mostly", 1.0)  # type: ignore[attr-defined]

            try:
                # Access DataFrame through execution engine's active batch
                df = execution_engine.batch_manager.active_batch.data.dataframe
                spark = df.sparkSession

                # Compute current year bounds using Spark SQL
                bounds_row = spark.sql("""
                    SELECT
                        DATE_TRUNC('YEAR', CURRENT_DATE()) as year_start,
                        DATE_TRUNC('YEAR', CURRENT_DATE()) + INTERVAL '1 YEAR' - INTERVAL '1 DAY' as year_end
                """).first()

                year_start = bounds_row["year_start"]
                year_end = bounds_row["year_end"]

                # Count total non-null date values
                non_null_count = df.filter(F.col(column).isNotNull()).count()

                if non_null_count == 0:
                    return {
                        "success": True,
                        "result": {
                            "element_count": 0,
                            "unexpected_count": 0,
                            "unexpected_percent": 0.0,
                            "partial_unexpected_list": [],
                            "observed_value": "Column is entirely NULL",
                        },
                    }

                # Find dates outside the current year
                out_of_range_df = df.filter(
                    F.col(column).isNotNull()
                    & ((F.col(column) < year_start) | (F.col(column) > year_end))
                )

                unexpected_count = out_of_range_df.count()
                unexpected_percent = (
                    (unexpected_count / non_null_count * 100) if non_null_count > 0 else 0.0
                )

                # Collect sample of out-of-range values
                unexpected_values = []
                if unexpected_count > 0:
                    sample_rows = out_of_range_df.select(column).limit(20).collect()
                    unexpected_values = [str(row[column]) for row in sample_rows]

                # Calculate success based on mostly threshold
                success_percent = (
                    1.0 - (unexpected_count / non_null_count) if non_null_count > 0 else 1.0
                )
                success = success_percent >= mostly

                return {
                    "success": success,
                    "result": {
                        "element_count": non_null_count,
                        "unexpected_count": unexpected_count,
                        "unexpected_percent": unexpected_percent,
                        "partial_unexpected_list": unexpected_values[:10],
                        "observed_value": f"{success_percent * 100:.2f}% of dates within {year_start} to {year_end}",
                    },
                }

            except Exception as e:
                logger.exception(f"Failed to execute current year validation: {e}")
                return {
                    "success": False,
                    "result": {
                        "element_count": 0,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "observed_value": f"Validation failed: {e!s}",
                    },
                }
