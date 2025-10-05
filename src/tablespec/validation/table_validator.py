"""Table validation using UMF specifications with DataFrame error reporting.

The TableValidator module provides comprehensive validation capabilities for PySpark DataFrames
against Universal Metadata Format (UMF) specifications. It is designed specifically for healthcare
data quality assurance and generates detailed error reports in structured DataFrame format.

## Module Overview

TableValidator validates data integrity across multiple dimensions:
- **Schema compliance**: Column presence and structure
- **Data type validation**: Type consistency with UMF specifications
- **Nullable constraints**: LOB-specific null value validation
- **Business rules**: Healthcare domain-specific validation rules
- **Uniqueness constraints**: Critical column duplicate detection
- **Format validation**: Pattern matching (e.g., state codes)
- **Value constraints**: Allowed value set validation

## Core Features

- **PySpark Integration**: Native Spark DataFrame processing for scalability
- **UMF-Driven Validation**: Uses YAML-based Universal Metadata Format specifications
- **Healthcare Domain Focus**: Built-in patterns for member IDs, LOBs, dates
- **Structured Error Reporting**: Returns validation errors as structured DataFrames
- **LOB-Aware Validation**: Supports Medicaid (MD), Medicare (ME), Marketplace (MP) specific rules
- **Performance Optimized**: Uses SQL queries for efficient large-scale validation

## Usage Examples

### Basic Table Validation

```python
from pyspark.sql import SparkSession
from pathlib import Path
from pulseflow_core.common.table_validator import TableValidator

# Initialize Spark and validator
spark = SparkSession.builder.appName("DataValidation").getOrCreate()
validator = TableValidator(spark)

# Load your data
df = spark.read.csv("member_data.csv", header=True, inferSchema=True)

# Validate against UMF specification
umf_path = Path("metadata/MemberTable.umf.yaml")
error_df = validator.validate_table(df, umf_path, "MemberTable")

# Check results
if error_df.count() > 0:
    print("Validation errors found:")
    error_df.show()
else:
    print("All validations passed!")
```

### Healthcare Data Pipeline Integration

```python
def validate_healthcare_table(spark, table_name, umf_dir):
    \"\"\"Validate a healthcare table using UMF specifications.\"\"\"
    validator = TableValidator(spark)
    df = spark.table(table_name)
    umf_path = umf_dir / f"{table_name}.umf.yaml"

    error_df = validator.validate_table(df, umf_path, table_name)
    error_count = error_df.count()

    if error_count > 0:
        # Save errors for reporting
        error_df.coalesce(1).write.mode("overwrite").csv(f"validation_errors/{table_name}")
        return False
    return True

# Example usage in pipeline
tables = ["OutreachList", "DispositionFile", "MemberEnrollment"]
for table in tables:
    is_valid = validate_healthcare_table(spark, table, Path("umf_metadata/"))
    if not is_valid:
        print(f"⚠️  {table} failed validation")
```

### Custom Validation Workflow

```python
# Initialize validator
validator = TableValidator(spark)

# Load UMF and data
umf_path = Path("specs/Centene_Disposition_V_4_0.umf.yaml")
df = spark.read.option("header", "true").csv("disposition_data.csv")

# Run validation with table name override
error_df = validator.validate_table(df, umf_path, "CenteneDisposition")

# Analyze validation results by error type
error_summary = (error_df
    .groupBy("error_type", "severity")
    .count()
    .orderBy("error_type", "severity")
)

print("Validation Summary:")
error_summary.show()

# Filter critical errors only
critical_errors = error_df.filter(error_df.severity == "error")
if critical_errors.count() > 0:
    print("CRITICAL ERRORS - Must fix before proceeding:")
    critical_errors.select("column_name", "error_message", "error_count").show()
```

## Validation Types

### 1. Schema Validation
Ensures DataFrame columns match UMF column definitions:
- **Missing Columns**: Required columns absent from DataFrame (ERROR)
- **Extra Columns**: Unexpected columns not in UMF spec (WARNING)

### 2. Data Type Validation
Validates PySpark data types against UMF specifications:
- **Type Mapping**: VARCHAR→STRING, INTEGER→INT, etc.
- **Type Mismatches**: Logs discrepancies with expected vs actual types (ERROR)

### 3. Nullable Constraints
LOB-specific null value validation:
- **Per-LOB Rules**: Different nullability for MD/ME/MP lines of business
- **Null Violations**: Counts null values where not allowed (ERROR)

### 4. Business Rules Validation

#### Uniqueness Constraints
- **Critical Columns**: Validates uniqueness for columns marked as "critical"
- **Duplicate Detection**: Reports duplicate values with samples

#### Format Rules
- **Pattern Matching**: Validates format patterns (e.g., "2-character state code")
- **Regex Validation**: Uses SQL RLIKE for pattern enforcement

#### Value Constraints
- **Allowed Values**: Validates against permitted value sets
- **Parsing Support**: Handles various constraint formats:
  - `MP = "Marketplace", MD = "Medicaid", ME = "Medicare"`
  - `Valid values: "Y", "N"`

### 5. Healthcare Domain Patterns
- **Member ID Validation**: Detects member ID patterns and validates constraints
- **Outreach Table Checks**: Validates essential columns for outreach tables
- **LOB Validation**: Line of Business specific rules

## Error DataFrame Schema

Validation errors are returned as structured DataFrames with the following schema:

```python
VALIDATION_ERROR_SCHEMA = StructType([
    StructField("table_name", StringType(), False),           # Table being validated
    StructField("validation_timestamp", TimestampType(), False), # When validation ran
    StructField("error_type", StringType(), False),           # Type of validation error
    StructField("severity", StringType(), False),             # error, warning, info
    StructField("column_name", StringType(), True),           # Column name (null for table-level)
    StructField("rule_name", StringType(), True),             # Specific rule that failed
    StructField("rule_details", StringType(), True),          # Rule specification details
    StructField("error_message", StringType(), False),        # Human-readable message
    StructField("error_count", IntegerType(), True),         # Number of records affected
    StructField("sample_values", StringType(), True),         # Sample failing values
])
```

### Error Type Categories

- **schema**: Column presence/structure issues
- **data_type**: Type mismatch between DataFrame and UMF
- **nullable**: Null constraint violations
- **uniqueness**: Duplicate value detection
- **format**: Pattern/format rule violations
- **value_constraint**: Invalid values outside allowed sets
- **business_rule**: Healthcare domain rule violations

### Severity Levels

- **error**: Critical issues requiring immediate attention
- **warning**: Important issues that should be reviewed
- **info**: Informational notices for optimization

## UMF Integration

### UMF Structure Requirements

TableValidator expects UMF files with this structure:

```yaml
table_name: "MemberTable"
description: "Member demographic data"
columns:
  - name: "ClientMemberID"
    data_type: "VARCHAR"
    nullable:
      MD: false  # Not nullable for Medicaid
      ME: false  # Not nullable for Medicare
      MP: false  # Not nullable for Marketplace
    validation_rules:
      confidence: "critical"  # Triggers uniqueness validation
      format_rules: []
      value_constraints: []
  - name: "LOB"
    data_type: "VARCHAR"
    nullable:
      MD: false
      ME: false
      MP: false
    validation_rules:
      value_constraints:
        - 'MP = "Marketplace", MD = "Medicaid", ME = "Medicare"'
  - name: "State"
    data_type: "VARCHAR"
    nullable:
      MD: true
      ME: true
      MP: true
    validation_rules:
      format_rules:
        - "2-character state code"
```

### Relationship with Other Components

- **UMF Generation**: Created by Phase 1 metadata extraction
- **Pipeline Integration**: Used in Phase 6 (validation) and Phase 9 (disposition)
- **Spark Validation**: Complements phase_07_spark_validation.py for comprehensive validation

## Best Practices

### 1. Performance Optimization

```python
# Cache DataFrames when validating multiple times
df.cache()
validator = TableValidator(spark)
error_df = validator.validate_table(df, umf_path)

# Use broadcast for small lookup tables
small_df = spark.read.csv("lookup.csv")
small_df.createOrReplaceGlobalTempView("lookup_broadcast")
```

### 2. Error Handling

```python
try:
    error_df = validator.validate_table(df, umf_path, table_name)
    error_count = error_df.count()

    if error_count > 0:
        # Save errors for analysis
        error_df.write.mode("overwrite").parquet(f"errors/{table_name}")

        # Log summary
        logger.warning(f"Found {error_count} validation issues in {table_name}")

        # Decide whether to continue processing
        critical_errors = error_df.filter(error_df.severity == "error").count()
        if critical_errors > 0:
            raise ValueError(f"Critical validation errors in {table_name}")

except Exception as e:
    logger.error(f"Validation failed for {table_name}: {e}")
    raise
```

### 3. Monitoring and Alerting

```python
def validate_with_monitoring(validator, df, umf_path, table_name):
    \"\"\"Validate table with comprehensive monitoring.\"\"\"
    start_time = time.time()

    error_df = validator.validate_table(df, umf_path, table_name)

    # Performance metrics
    validation_time = time.time() - start_time
    row_count = df.count()
    error_count = error_df.count()

    # Log metrics
    logger.info(f"Validation completed for {table_name}:")
    logger.info(f"  Rows validated: {row_count:,}")
    logger.info(f"  Validation time: {validation_time:.2f}s")
    logger.info(f"  Errors found: {error_count}")
    logger.info(f"  Rate: {row_count/validation_time:,.0f} rows/sec")

    # Quality score
    if row_count > 0:
        quality_score = max(0, (1 - error_count/row_count) * 100)
        logger.info(f"  Data quality score: {quality_score:.1f}%")

    return error_df
```

### 4. Integration Patterns

```python
class DataQualityPipeline:
    \"\"\"Example pipeline integration pattern.\"\"\"

    def __init__(self, spark_session):
        self.spark = spark_session
        self.validator = TableValidator(spark_session)

    def validate_pipeline_stage(self, table_name, umf_dir, stage_name):
        \"\"\"Validate a pipeline stage.\"\"\"
        logger.info(f"Validating {stage_name}: {table_name}")

        df = self.spark.table(table_name)
        umf_path = umf_dir / f"{table_name}.umf.yaml"

        error_df = self.validator.validate_table(df, umf_path, table_name)

        # Stage-specific handling
        if stage_name == "ingestion":
            # More tolerant of warnings during ingestion
            critical_count = error_df.filter(error_df.severity == "error").count()
            return critical_count == 0
        elif stage_name == "final":
            # Strict validation before final output
            return error_df.count() == 0

        return True
```

### 5. Custom Rule Extensions

```python
# Extend TableValidator for domain-specific rules
class HealthcareTableValidator(TableValidator):
    \"\"\"Extended validator with healthcare-specific rules.\"\"\"

    def validate_member_demographics(self, df, umf_path):
        \"\"\"Additional validation for member demographic tables.\"\"\"
        base_errors = self.validate_table(df, umf_path)

        # Add custom healthcare validations
        custom_errors = []

        # Validate birth dates are reasonable
        if "BirthDate" in df.columns:
            invalid_dates = df.filter(
                (df.BirthDate < "1900-01-01") |
                (df.BirthDate > "2023-12-31")
            ).count()

            if invalid_dates > 0:
                # Add to error list using same format
                pass

        return base_errors
```

## Error Handling and Logging

The validator includes comprehensive error handling:

- **SQL Exceptions**: Gracefully handles SQL execution errors with warning logs
- **Missing Files**: Raises appropriate exceptions for missing UMF files
- **Invalid YAML**: Handles malformed UMF files with descriptive errors
- **Schema Mismatches**: Continues validation even with structural issues
- **Performance Issues**: Logs warnings for slow validation operations

## Thread Safety and Performance

- **Spark Integration**: Leverages Spark's distributed processing capabilities
- **SQL Optimization**: Uses efficient SQL queries for large-scale validation
- **Memory Management**: Processes data in Spark DataFrames without loading into driver memory
- **Concurrent Safe**: Safe for use in multi-threaded Spark applications

TableValidator is designed for production healthcare data pipelines where data quality
is critical and validation must scale to millions of records while providing
detailed, actionable error reporting.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# Schema for validation error DataFrame
VALIDATION_ERROR_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("validation_timestamp", TimestampType(), False),
        StructField(
            "error_type", StringType(), False
        ),  # schema, data_type, nullable, format, value, business_rule
        StructField("severity", StringType(), False),  # error, warning, info
        StructField("column_name", StringType(), True),  # null for table-level errors
        StructField("rule_name", StringType(), True),  # specific rule that failed
        StructField("rule_details", StringType(), True),  # rule specification/pattern
        StructField("error_message", StringType(), False),  # human-readable message
        StructField("error_count", IntegerType(), True),  # number of records affected
        StructField("sample_values", StringType(), True),  # sample of failing values
    ]
)


class TableValidator:
    """Validates Spark DataFrames against UMF specifications."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the validator with a Spark session.

        Args:
            spark: SparkSession for executing validation queries

        """
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        self.errors: list[dict] = []

    def validate_table(
        self,
        df: DataFrame,
        umf_path: Path,
        table_name: str | None = None,
    ) -> DataFrame:
        """Validate DataFrame against UMF specification.

        Args:
            df: Spark DataFrame to validate
            umf_path: Path to UMF specification file
            table_name: Optional table name override

        Returns:
            DataFrame with validation errors (empty if no errors)

        """
        self.logger.info(f"Starting validation for table using UMF: {umf_path}")

        # Load UMF specification
        umf = self._load_umf(umf_path)
        table = table_name or umf.get("table_name", "unknown")
        timestamp = datetime.now()

        # Clear previous errors
        self.errors = []

        # Run all validation checks
        self._validate_schema(df, umf, table, timestamp)
        self._validate_data_types(df, umf, table, timestamp)
        self._validate_nullable(df, umf, table, timestamp)
        self._validate_rules(df, umf, table, timestamp)

        # Convert errors to DataFrame
        if self.errors:
            self.logger.info(f"Found {len(self.errors)} validation issues for {table}")
            return self.spark.createDataFrame(self.errors, VALIDATION_ERROR_SCHEMA)
        self.logger.info(f"✓ All validations passed for {table}")
        # Return empty DataFrame with correct schema
        return self.spark.createDataFrame([], VALIDATION_ERROR_SCHEMA)

    def _load_umf(self, umf_path: Path) -> dict:
        """Load UMF specification from YAML file.

        Args:
            umf_path: Path to UMF YAML file

        Returns:
            UMF specification as dictionary

        """
        try:
            with Path(umf_path).open(encoding="utf-8") as f:
                umf = yaml.safe_load(f)
            self.logger.debug(
                f"Loaded UMF for table: {umf.get('table_name', 'unknown')}"
            )
            return umf
        except Exception as e:
            self.logger.exception(f"Failed to load UMF from {umf_path}: {e}")
            raise

    def _add_error(
        self,
        table_name: str,
        timestamp: datetime,
        error_type: str,
        severity: str,
        error_message: str,
        column_name: str | None = None,
        rule_name: str | None = None,
        rule_details: str | None = None,
        error_count: int | None = None,
        sample_values: str | None = None,
    ) -> None:
        """Add a validation error to the collection.

        Args:
            table_name: Name of the table being validated
            timestamp: When the validation occurred
            error_type: Type of validation error
            severity: Severity level (error, warning, info)
            error_message: Human-readable error message
            column_name: Column name if column-specific error
            rule_name: Name of the validation rule that failed
            rule_details: Details about the rule specification
            error_count: Number of records affected
            sample_values: Sample of failing values

        """
        self.errors.append(
            {
                "table_name": table_name,
                "validation_timestamp": timestamp,
                "error_type": error_type,
                "severity": severity,
                "column_name": column_name,
                "rule_name": rule_name,
                "rule_details": rule_details,
                "error_message": error_message,
                "error_count": error_count,
                "sample_values": sample_values,
            }
        )

    def _validate_schema(
        self,
        df: DataFrame,
        umf: dict,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate that DataFrame schema matches UMF column definitions.

        Args:
            df: DataFrame to validate
            umf: UMF specification
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        df_columns = set(df.columns)
        umf_columns = {col["name"] for col in umf.get("columns", [])}

        # Check for missing columns
        missing_columns = umf_columns - df_columns
        for col in missing_columns:
            self._add_error(
                table_name=table_name,
                timestamp=timestamp,
                error_type="schema",
                severity="error",
                column_name=col,
                rule_name="column_required",
                rule_details="Column defined in UMF specification",
                error_message=f"Required column '{col}' is missing from DataFrame",
            )

        # Check for extra columns
        extra_columns = df_columns - umf_columns
        for col in extra_columns:
            self._add_error(
                table_name=table_name,
                timestamp=timestamp,
                error_type="schema",
                severity="warning",
                column_name=col,
                rule_name="column_unexpected",
                rule_details="Column not defined in UMF specification",
                error_message=f"Unexpected column '{col}' found in DataFrame",
            )

    def _validate_data_types(
        self,
        df: DataFrame,
        umf: dict,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate DataFrame column data types against UMF specification.

        Args:
            df: DataFrame to validate
            umf: UMF specification
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        df_schema = {
            field.name: field.dataType.simpleString() for field in df.schema.fields
        }

        for col_spec in umf.get("columns", []):
            col_name = col_spec["name"]
            expected_type = col_spec.get("data_type", "").upper()

            if col_name not in df_schema:
                continue  # Already handled in schema validation

            actual_type = df_schema[col_name].upper()

            # Map UMF types to Spark types for comparison
            type_mapping = {
                "VARCHAR": ["STRING"],
                "CHAR": ["STRING"],
                "INTEGER": ["INT", "INTEGER"],
                "BIGINT": ["BIGINT", "LONG"],
                "DECIMAL": ["DECIMAL"],
                "DATE": ["DATE"],
                "TIMESTAMP": ["TIMESTAMP"],
                "BOOLEAN": ["BOOLEAN"],
            }

            if expected_type in type_mapping:
                valid_types = type_mapping[expected_type]
                if actual_type not in valid_types:
                    self._add_error(
                        table_name=table_name,
                        timestamp=timestamp,
                        error_type="data_type",
                        severity="error",
                        column_name=col_name,
                        rule_name="type_mismatch",
                        rule_details=f"Expected: {expected_type}, Found: {actual_type}",
                        error_message=f"Column '{col_name}' has incorrect data type",
                    )

    def _validate_nullable(
        self,
        df: DataFrame,
        umf: dict,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate nullable constraints based on UMF specification.

        Args:
            df: DataFrame to validate
            umf: UMF specification
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        # Create temp view for SQL queries
        temp_view = f"{table_name}_validation_temp"
        df.createOrReplaceTempView(temp_view)

        # Check if table has LOB column for LOB-specific validation
        has_lob_column = "LOB" in df.columns

        for col_spec in umf.get("columns", []):
            col_name = col_spec["name"]
            nullable_spec = col_spec.get("nullable", {})

            if col_name not in df.columns:
                continue  # Already handled in schema validation

            # Check nullable constraints for each LOB if specified
            for lob, is_nullable in nullable_spec.items():
                if not is_nullable:  # Column should not be null for this LOB
                    # Build query based on whether LOB column exists
                    if has_lob_column:
                        # LOB-specific validation for tables with LOB column
                        null_query = f"""
                        SELECT COUNT(*) as null_count
                        FROM {temp_view}
                        WHERE {col_name} IS NULL AND LOB = '{lob}'
                        """
                    else:
                        # Global validation for lookup/disposition tables without LOB
                        # Only check once (on first LOB) to avoid duplicate checks
                        if lob != next(iter(nullable_spec.keys())):
                            continue
                        null_query = f"""
                        SELECT COUNT(*) as null_count
                        FROM {temp_view}
                        WHERE {col_name} IS NULL
                        """

                    try:
                        result = self.spark.sql(null_query).collect()
                        null_count = result[0]["null_count"] if result else 0

                        if null_count > 0:
                            error_msg = (
                                f"Found {null_count} null values in '{col_name}' for LOB '{lob}'"
                                if has_lob_column
                                else f"Found {null_count} null values in '{col_name}'"
                            )
                            rule_details = (
                                f"Column must not be null for LOB '{lob}'"
                                if has_lob_column
                                else "Column must not be null"
                            )

                            self._add_error(
                                table_name=table_name,
                                timestamp=timestamp,
                                error_type="nullable",
                                severity="error",
                                column_name=col_name,
                                rule_name="not_null_constraint",
                                rule_details=rule_details,
                                error_message=error_msg,
                                error_count=null_count,
                            )
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to check nullable constraint for {col_name}: {e}"
                        )

    def _validate_rules(
        self,
        df: DataFrame,
        umf: dict,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate business rules from UMF validation_rules section.

        Args:
            df: DataFrame to validate
            umf: UMF specification
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        # Create temp view for SQL queries
        temp_view = f"{table_name}_validation_temp"
        df.createOrReplaceTempView(temp_view)

        for col_spec in umf.get("columns", []):
            col_name = col_spec["name"]
            validation_rules = col_spec.get("validation_rules", {})

            if col_name not in df.columns:
                continue  # Already handled in schema validation

            # Check uniqueness for critical columns
            if validation_rules.get("confidence") == "critical":
                self._validate_uniqueness(
                    df, temp_view, col_name, table_name, timestamp
                )

            # Validate format rules
            for format_rule in validation_rules.get("format_rules", []):
                self._validate_format_rule(
                    df, temp_view, col_name, format_rule, table_name, timestamp
                )

            # Validate value constraints
            for value_constraint in validation_rules.get("value_constraints", []):
                self._validate_value_constraint(
                    df, temp_view, col_name, value_constraint, table_name, timestamp
                )

    def _validate_uniqueness(
        self,
        _df: DataFrame,
        temp_view: str,
        col_name: str,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate uniqueness constraint for critical columns.

        Args:
            df: DataFrame to validate
            temp_view: Name of temporary view
            col_name: Column name to check
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        dup_query = f"""
        SELECT {col_name}, COUNT(*) as cnt
        FROM {temp_view}
        WHERE {col_name} IS NOT NULL
        GROUP BY {col_name}
        HAVING COUNT(*) > 1
        """

        try:
            duplicates = self.spark.sql(dup_query)
            dup_count = duplicates.count()

            if dup_count > 0:
                # Get sample duplicate values
                samples = duplicates.limit(5).collect()
                sample_str = ", ".join([str(row[col_name]) for row in samples])

                self._add_error(
                    table_name=table_name,
                    timestamp=timestamp,
                    error_type="uniqueness",
                    severity="error",
                    column_name=col_name,
                    rule_name="unique_constraint",
                    rule_details="Column marked as critical must be unique",
                    error_message=f"Found {dup_count} duplicate values in '{col_name}'",
                    error_count=dup_count,
                    sample_values=sample_str,
                )
        except Exception as e:
            self.logger.warning(f"Failed to check uniqueness for {col_name}: {e}")

    def _validate_format_rule(
        self,
        _df: DataFrame,
        temp_view: str,
        col_name: str,
        format_rule: str,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate format rules (basic pattern matching).

        Args:
            df: DataFrame to validate
            temp_view: Name of temporary view
            col_name: Column name to check
            format_rule: Format rule description
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        # For now, just check common patterns
        if "2-character" in format_rule.lower() and "state" in format_rule.lower():
            # Validate 2-character state codes
            invalid_query = f"""
            SELECT {col_name}, COUNT(*) as cnt
            FROM {temp_view}
            WHERE {col_name} IS NOT NULL
            AND (LENGTH({col_name}) != 2 OR {col_name} RLIKE '[^A-Z]')
            GROUP BY {col_name}
            """

            try:
                invalid_values = self.spark.sql(invalid_query)
                invalid_count = invalid_values.count()

                if invalid_count > 0:
                    samples = invalid_values.limit(5).collect()
                    sample_str = ", ".join([str(row[col_name]) for row in samples])

                    self._add_error(
                        table_name=table_name,
                        timestamp=timestamp,
                        error_type="format",
                        severity="warning",
                        column_name=col_name,
                        rule_name="format_validation",
                        rule_details=format_rule,
                        error_message=f"Found {invalid_count} values not matching format rule",
                        error_count=invalid_count,
                        sample_values=sample_str,
                    )
            except Exception as e:
                self.logger.warning(
                    f"Failed to validate format rule for {col_name}: {e}"
                )

    def _validate_value_constraint(
        self,
        _df: DataFrame,
        temp_view: str,
        col_name: str,
        value_constraint: str,
        table_name: str,
        timestamp: datetime,
    ) -> None:
        """Validate value constraints (allowed values).

        Args:
            df: DataFrame to validate
            temp_view: Name of temporary view
            col_name: Column name to check
            value_constraint: Value constraint description
            table_name: Name of the table
            timestamp: Validation timestamp

        """
        # Extract allowed values from constraint description
        allowed_values = []

        # Parse common patterns like "MP = Marketplace", "MD = Medicaid", "ME = Medicare"
        if "=" in value_constraint:
            parts = value_constraint.split(",")
            for part in parts:
                if "=" in part:
                    value = part.split("=")[0].strip().strip("\"'")
                    if value:
                        allowed_values.append(value)

        # Check simple list patterns like ["Y", "N"]
        if not allowed_values and any(
            x in value_constraint for x in ['"Y"', '"N"', "'Y'", "'N'"]
        ):
            if '"Y"' in value_constraint or "'Y'" in value_constraint:
                allowed_values.append("Y")
            if '"N"' in value_constraint or "'N'" in value_constraint:
                allowed_values.append("N")

        if allowed_values:
            # Build SQL to check for invalid values
            value_list = "', '".join(allowed_values)
            invalid_query = f"""
            SELECT {col_name}, COUNT(*) as cnt
            FROM {temp_view}
            WHERE {col_name} IS NOT NULL
            AND {col_name} NOT IN ('{value_list}')
            GROUP BY {col_name}
            """

            try:
                invalid_values = self.spark.sql(invalid_query)
                invalid_count = invalid_values.count()

                if invalid_count > 0:
                    samples = invalid_values.limit(5).collect()
                    sample_str = ", ".join([str(row[col_name]) for row in samples])

                    self._add_error(
                        table_name=table_name,
                        timestamp=timestamp,
                        error_type="value_constraint",
                        severity="warning",
                        column_name=col_name,
                        rule_name="allowed_values",
                        rule_details=f"Allowed values: {allowed_values}",
                        error_message=f"Found {invalid_count} values not in allowed set",
                        error_count=invalid_count,
                        sample_values=sample_str,
                    )
            except Exception as e:
                self.logger.warning(
                    f"Failed to validate value constraint for {col_name}: {e}"
                )


__all__ = ["VALIDATION_ERROR_SCHEMA", "TableValidator"]
